import datetime as dt
import pathlib
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor

import storage

from agent_layer.contracts import (
    Invocation,
    Principal,
    ProtectedItem,
    ProviderRequest,
    ProviderResult,
    Scope,
    SecurityError,
)
from agent_layer.persistence import AgentRepository, stable_hash
from agent_layer.providers import DeterministicProvider, ProviderRegistry
from agent_layer.runtime import AgentRuntime, CAPABILITY_CATALOG
from agent_layer.security import ContextAssembler, derive_composed_scope


class AgentLayerFixture(unittest.TestCase):
    def setUp(self):
        # Leave accepted Stage 2 tables intact: its scheduler may be running in
        # cumulative regression.  Reset only the additive Agent Layer schema.
        for table in reversed(storage.Base.metadata.sorted_tables):
            if table.name.startswith("al_"):
                table.drop(storage.ENGINE, checkfirst=True)
        storage.init_db()
        self.repo = AgentRepository()
        self.runtime = AgentRuntime(self.repo, runtime_id="test-runtime")
        self.alice = Principal("alice", "user", Scope(client_id=10, project_code="A"))
        self.bob = Principal("bob", "user", Scope(client_id=20, project_code="B"))
        self.authority_service = Principal("fixture-authority", "service", Scope())
        self.owner = Principal("owner", "user", Scope())
        self.verifier = Principal("independent-verifier", "service", Scope())
        self.security_owner = Principal("security-owner", "service", Scope())
        principals = {
            "fixture-authority": self._actor("service", "authority", (
                "capability.control", "authority.delegate", "provider.configure",
                "entitlement.configure")),
            "owner": self._actor("user", "owners", (
                "action.approve", "entitlement.configure", "security.contain",
                "authority.revoke")),
            "independent-verifier": self._actor("service", "verification", ("action.verify",)),
            "config-verifier": self._actor("service", "verification", ("action.verify",)),
            "security-verifier": self._actor("service", "verification", ("action.verify", "authority.revoke")),
            "security-owner": self._actor("service", "security", ("security.contain", "action.approve")),
            "alice": self._actor("user", "operators-a", ()),
            "bob": self._actor("user", "operators-b", ()),
        }
        principals["limited-delegator"] = self._actor(
            "service", "limited", ("authority.delegate",))
        principals["limited-delegator"]["max_autonomy"] = 2
        for key in ("platform", "client:10", "client:10/project:A",
                    "client:20", "client:20/project:B"):
            self.repo.install_authority_value(
                "AB-AUTH-001", key, {"principals": principals},
                "TEST-AUTHORITY-MATRIX", "signed-fixture-loader", dt.datetime.utcnow(),
                proof_ref="fixture://signed/authority-matrix",
            )

    @staticmethod
    def _actor(principal_class, group, permissions):
        return {"principal_class": principal_class, "independence_group": group,
                "permissions": list(permissions), "capabilities": ["*"],
                "risk_classes": ["*"], "max_autonomy": 5, "actions": ["*"],
                "confidentiality": ["restricted", "internal", "public"],
                "industry_keys": ["construction"],
                "information_permissions": ["*"], "domains": ["*"]}

    def authorize(
        self, capability_id, principal=None, autonomy=2, domains=("SD3", "SD4"),
        actions=None, entitlement=False, autonomy_limits=None, shadow_allowed=False,
    ):
        principal = principal or self.alice
        definition = next(item for item in CAPABILITY_CATALOG if item.capability_id == capability_id)
        self.repo.set_capability_state(capability_id, self.authority_service, enabled=True, healthy=True)
        self.repo.grant_authority(
            "grant:%s:%s" % (principal.principal_id, capability_id),
            principal.principal_id, principal.principal_class, capability_id,
            authority_basis="TEST-AUTHORITY-1", client_id=principal.scope.client_id,
            project_code=principal.scope.project_code, max_autonomy=autonomy,
            information_permissions=("reason", "read"),
            action_permissions=actions or (definition.required_action,),
            allowed_domains=domains,
            granting_actor=self.authority_service,
            autonomy_limits=(autonomy_limits if autonomy_limits is not None else
                             ({"allow_all": True} if autonomy >= 4 else {})),
            shadow_allowed=shadow_allowed,
        )
        if entitlement:
            self.repo.assign_entitlement(
                "ent:%s:%s" % (principal.principal_id, capability_id),
                principal.scope.client_id, "capability", capability_id,
                {"enabled": True}, 1, "TEST-COMMERCIAL-AUTHORITY",
                principal.principal_id,
                granting_actor=self.authority_service,
            )

    def request_hash(self, invocation):
        return stable_hash({
            "capability_id": invocation.capability_id,
            "principal": invocation.principal.principal_id,
            "client_id": invocation.principal.scope.client_id,
            "project_code": invocation.principal.scope.project_code,
            "payload": invocation.payload,
            "requested_autonomy": invocation.requested_autonomy,
            "shadow_mode": invocation.shadow_mode,
        })

    def install_provider(self, provider_id, classes=("SD3",), training=False,
                         retention="zero-retention", retention_max=0):
        return self.repo.install_provider_policy(
            provider_id, 1, classes, training, retention, True,
            "TEST-AUTHORITY-MATRIX", granting_actor=self.authority_service,
            allowed_confidentiality=("restricted", "internal"),
            permitted_uses=("reason",), retention_max_seconds=retention_max,
            access_controls=("tenant-isolation",), allowed_regions=("test-region",),
            audit_required=True, attribution_required=True,
            deletion_supported=True, withdrawal_supported=True,
            allowed_distribution_uses=("internal",), terms_ref="fixture://provider-terms",
        )

    @staticmethod
    def protected_item(reference, value, client_id=10, project_code="A", domain="SD3"):
        return ProtectedItem(
            reference, value, domain, client_id, project_code,
            classification="controlled-test", confidentiality="restricted",
            permitted_uses=("reason",), provider_eligible=True,
            retention_max_seconds=0, region="test-region",
            access_requirements=("tenant-isolation",), deletion_required=True,
            withdrawal_required=True, distribution_uses=("internal",),
            provenance={"source_ref": "fixture://payload"},
        )

    @classmethod
    def protected_payload(cls, payload, client_id=10, project_code="A", domain="SD3"):
        return cls.protected_item("invocation-payload", dict(payload), client_id, project_code, domain)

    def approve(self, invocation, approver=None):
        approver = approver or self.owner
        approver_id = approver.principal_id if isinstance(approver, Principal) else approver
        return self.repo.record_approval(
            invocation.approval_key, approver_id, invocation.capability_id,
            invocation.principal.scope.client_id, self.request_hash(invocation),
            self.repo.capability(invocation.capability_id)["risk_class"],
            "APPROVED", "TEST-AUTHORITY-MATRIX",
            project_code=invocation.principal.scope.project_code,
            approver_actor=approver,
        )

    @staticmethod
    def reasoning_case(capability_id):
        cases = {
            "flo.industry.reason": ({"query": "interpret", "industry_key": "construction", "evidence_refs": ["e:1"]},
                                    {"interpretation": "scoped", "evidence_refs": ["e:1"], "inference_state": "inferred"}),
            "flo.client.reason": ({"query": "interpret", "evidence_refs": ["e:1"]},
                                  {"interpretation": "scoped", "evidence_refs": ["e:1"], "inference_state": "inferred"}),
            "takeon.propose": ({"phase": "client", "requested_outcomes": ["configured"], "discovered_requirements": {"role": "manager"}},
                               {"proposal": {"authoritative": False}, "consequences": [], "authority_status": "PROPOSAL_ONLY"}),
            "manager_pa.assist": ({"request": "help", "assistance_mode": "recommend", "evidence_refs": ["e:1"]},
                                  {"assistance_type": "recommendation", "content": "bounded", "evidence_refs": ["e:1"], "proposed_action": {"executed": False}}),
            "guardian.diagnose": ({"affected_component": "worker", "observations": ["slow"], "evidence_refs": ["e:1"]},
                                  {"diagnosis": "load", "evidence_refs": ["e:1"], "affected_scope": {"component": "worker"}, "confidence": {"value": .8}, "escalation": "none", "recommendation": "observe"}),
            "capacity.assess": ({"metric_samples": [{"value": 1}], "horizon": "7d", "evidence_refs": ["e:1"]},
                                {"assessment": "adequate", "evidence_refs": ["e:1"], "assumptions": [], "recommendation": "observe"}),
            "critical_path.analyze": ({"operational_refs": ["task:1"], "known_dependencies": [], "horizon": "7d"},
                                      {"known_facts": ["task:1"], "inferred_dependencies": [], "blockers": [], "risk": {"level": "low"}, "evidence_refs": ["task:1"]}),
            "consequence.analyze": ({"event_ref": "event:1", "time_horizon": "7d", "candidate_capabilities": ["stage2.invoke"]},
                                    {"consequence": "delay", "severity": "low", "confidence": {"value": .7}, "intervention": {"capability_id": "stage2.invoke"}, "required_authority": ["invoke"], "expected_benefit": "recovery", "risk": "bounded"}),
            "performance.analyze": ({"metric": "cycle", "window": "30d", "evidence_refs": ["e:1"]},
                                    {"prediction": {"direction": "stable"}, "evidence_refs": ["e:1"], "source_age": "1d", "comparison": {}, "confidence": {"value": .7}}),
            "provider.reason": ({"purpose": "analysis", "question": "status?", "evidence_refs": ["e:1"]},
                                {"analysis": {"status": "bounded"}, "evidence_refs": ["e:1"]}),
            "market.analyze": ({"query": "trend", "source_refs": ["source:1"], "observed_at": "2026-09-01T10:00:00Z", "retrieved_at": "2026-09-01T10:01:00Z"},
                               {"finding": "stable", "source_refs": ["source:1"], "observed_at": "2026-09-01T10:00:00Z", "retrieved_at": "2026-09-01T10:01:00Z", "recommendation": "review", "authorizes_action": False}),
        }
        return cases[capability_id]


class ArchitectureAndConfigurationTests(AgentLayerFixture):
    def test_catalog_covers_every_capability_family_and_explicit_edge_contracts(self):
        families = {definition.family for definition in CAPABILITY_CATALOG}
        expected = {
            "AL-CP-001", "AL-FLO-001", "AL-TAKEON-001", "AL-CFG-001",
            "AL-PA-001", "AL-PLAT-001", "AL-CAP-001", "AL-CHAN-001",
            "AL-OBJ-001", "AL-CRIT-001", "AL-CONS-001", "AL-PERF-001",
            "AL-LEARN-001", "AL-INTEL-001", "AL-DIST-001", "AL-ENT-001",
            "AL-AUTO-001", "AL-TOOL-001", "AL-GRAPH-001", "AL-VERIFY-001",
            "AL-PROV-001", "AL-NOAI-001", "AL-MARKET-001", "AL-HELP-001",
            "AL-SEC-001",
        }
        self.assertEqual(expected, families)
        for capability in self.repo.capabilities():
            contract = capability["contract"]
            self.assertTrue(contract["concurrency"])
            self.assertIn("claim_required", contract)
            self.assertTrue(contract["uncertain_outcome"])
            self.assertTrue(contract["no_agent_behavior"])
            self.assertFalse(capability["enabled"])

    def test_agent_layer_is_compositional_and_core_has_no_concrete_industry_dependency(self):
        core_source = pathlib.Path("core/conversation.py").read_text()
        adapter_source = pathlib.Path("agent_layer/stage2.py").read_text()
        self.assertNotIn("industries.construction", core_source)
        self.assertNotIn("ConstructionIndustryModule", core_source)
        self.assertNotIn("create_task", adapter_source)
        self.assertNotIn("SessionLocal", adapter_source)
        self.assertIn("authoritative_handler", adapter_source)

    def test_schema_is_additive_and_security_domains_are_separate(self):
        inspector = storage.inspect(storage.ENGINE)
        tables = set(inspector.get_table_names())
        self.assertTrue({
            "al_configurations", "al_capabilities", "al_authority_grants",
            "al_objectives", "al_work_units", "al_executions", "al_learning",
            "al_intelligence", "al_distributions", "al_security_events",
        }.issubset(tables))
        self.assertIn("tasks", tables)
        self.assertIn("conversation_states", tables)

    def test_authority_values_are_versioned_explicit_and_revocable(self):
        self.assertIsNone(self.repo.authority_value("AB-AUTH-017", ["client:10", "platform"]))
        row = self.repo.install_authority_value(
            "AB-AUTH-001", "client:10", {"roles": {"manager": ["help.discover"]}},
            "TEST-ROLE-MATRIX-1", "owner", dt.datetime.utcnow(),
            proof_ref="fixture://signed/role-matrix",
        )
        self.assertEqual(2, row["version"])
        self.assertEqual("TEST-ROLE-MATRIX-1", row["authority_instrument"])
        self.assertTrue(self.repo.revoke_authority_value("AB-AUTH-001", "client:10", "owner"))
        self.assertIsNone(self.repo.authority_value("AB-AUTH-001", ["client:10"]))

    def test_configuration_proposal_commit_introspection_idempotency_and_revocation(self):
        proposal = self.repo.propose_configuration(
            "client:10", {"reasoning": {"depth": "bounded"}, "disabled_capabilities": []},
            "alice", "cfg-1", client_id=10, reason="controlled fixture",
        )
        self.assertEqual("proposed", proposal["status"])
        self.assertEqual(proposal, self.repo.propose_configuration(
            "client:10", {"reasoning": {"depth": "bounded"}, "disabled_capabilities": []},
            "alice", "cfg-1", client_id=10,
        ))
        committed = self.repo.commit_configuration(
            "client:10", proposal["version"], None, "owner", "TEST-CONFIG-AUTHORITY", 10
        )
        self.assertEqual("effective", committed["status"])
        effective = self.repo.effective_configuration(["platform", "client:10"], 10)
        self.assertEqual("bounded", effective["effective"]["reasoning"]["depth"])
        self.assertEqual([{"subject_key": "client:10", "version": 1}], effective["provenance"])
        self.assertTrue(self.repo.revoke_configuration("client:10", 1, "owner"))
        self.assertEqual({}, self.repo.effective_configuration(["client:10"], 10)["effective"])

    def test_stale_configuration_conflict_preserves_current_effective_state(self):
        first = self.repo.propose_configuration("client:10", {"mode": "one"}, "alice", "cfg-a", 10)
        self.repo.commit_configuration("client:10", first["version"], None, "owner", "AUTH", 10)
        stale = self.repo.propose_configuration("client:10", {"mode": "stale"}, "alice", "cfg-b", 10)
        current = self.repo.propose_configuration("client:10", {"mode": "current"}, "alice", "cfg-c", 10)
        self.repo.commit_configuration("client:10", current["version"], 1, "owner", "AUTH", 10)
        with self.assertRaisesRegex(Exception, "stale"):
            self.repo.commit_configuration("client:10", stale["version"], 1, "owner", "AUTH", 10)
        self.assertEqual("current", self.repo.effective_configuration(["client:10"], 10)["effective"]["mode"])

    def test_recomposition_intersects_constraints_and_denies_cross_client(self):
        composed = derive_composed_scope(self.alice, [
            {"client_id": 10, "project_code": "A", "domains": ["SD3", "SD4"],
             "max_autonomy": 4, "distributable": True, "provider_eligible": True},
            {"client_id": 10, "project_code": "A", "domains": ["SD4"],
             "max_autonomy": 2, "distributable": False, "provider_eligible": True},
        ])
        self.assertEqual(["SD4"], composed["domains"])
        self.assertEqual(2, composed["max_autonomy"])
        self.assertFalse(composed["distributable"])
        with self.assertRaises(SecurityError):
            derive_composed_scope(self.alice, [{"client_id": 20, "domains": ["SD3"]}])

    def test_takeon_and_evolution_are_proposals_until_canonical_authority_exists(self):
        self.authorize("takeon.propose")
        self.runtime.providers.register(DeterministicProvider("takeon", "1", lambda request: {"result": "proposal"}))
        proposal = self.runtime.takeon_proposal(
            self.alice, "client", {"roles": ["manager"], "projects": ["A"]},
            "takeon-proposal", "controlled discovery",
        )
        self.assertEqual("VALIDATED-PROPOSAL", proposal["status"])
        self.assertFalse(proposal["authoritative"])
        blocked = self.runtime.takeon_commit_eligibility(self.alice)
        self.assertEqual("AUTHORITY VALUE REQUIRED", blocked["status"])
        self.repo.install_authority_value(
            "AB-AUTH-017", "client:10", {"instrument": "fixture"},
            "TEST-TAKEON-AUTHORITY", "owner", dt.datetime.utcnow(),
            proof_ref="fixture://signed/takeon",
        )
        self.assertTrue(self.runtime.takeon_commit_eligibility(self.alice)["enabled"])

    def test_autonomy_and_learning_cannot_self_promote_or_invent_policy(self):
        self.assertEqual("SELF_PROMOTION_DENIED", self.runtime.autonomy_promotion_eligibility(
            self.alice, "alice", "stage2.invoke", 4
        )["reason"])
        self.assertEqual("AUTHORITY VALUE REQUIRED", self.runtime.autonomy_promotion_eligibility(
            self.alice, "service-worker", "stage2.invoke", 4
        )["status"])
        self.assertEqual("CROSS_CLIENT_GENERALIZATION_DISABLED", self.runtime.learning_promotion_eligibility(
            self.alice, "client", "platform"
        )["reason"])

    def test_market_intelligence_requires_attribution_and_never_authorizes_action(self):
        rejected = self.runtime.validate_external_intelligence({"source": "primary"})
        self.assertEqual("REJECTED", rejected["status"])
        valid = self.runtime.validate_external_intelligence({
            "source": "primary", "observed_at": "2026-09-01T10:00:00Z",
            "retrieved_at": "2026-09-01T10:01:00Z", "content_hash": "abc",
        })
        self.assertEqual("VALIDATED_EXTERNAL_EVIDENCE", valid["status"])
        self.assertFalse(valid["authorizes_action"])


class SecurityAndProviderTests(AgentLayerFixture):
    def test_context_assembly_denies_cross_client_project_domain_and_secret(self):
        assembler = ContextAssembler()
        policy = self.install_provider("assembler")
        good = self.protected_item("task:1", {"status": "open"})
        self.assertEqual({"task:1": {"status": "open"}}, assembler.assemble(
            self.alice, [good], ["SD3"], "reason", True, policy
        ))
        negatives = [
            ProtectedItem("other", "private", "SD3", 20, "B", permitted_uses=("reason",), provider_eligible=True),
            ProtectedItem("source", "code", "SD1", 10, "A", permitted_uses=("reason",), provider_eligible=True),
            ProtectedItem("secret", "sk-" + ("a" * 26), "SD2", 10, "A", permitted_uses=("reason",), provider_eligible=True),
        ]
        for item in negatives:
            with self.assertRaises(SecurityError):
                assembler.assemble(self.alice, [item], ["SD3"], "reason", True, policy)

    def test_secret_is_rejected_from_learning_intelligence_and_audit(self):
        with self.assertRaises(SecurityError):
            self.repo.persist_learning("secret", "client", ["obs"],
                                       {"api_key": "not-permitted"}, {}, "TEST-RETENTION", client_id=10)
        with self.assertRaises(SecurityError):
            self.repo.persist_intelligence("secret", "finding", ["obs"],
                                           {"password": "not-permitted"}, {}, "TEST-RETENTION", client_id=10)
        audit_id = self.repo.audit("security", "alice", "DENIED",
                                   {"value": "Bearer " + ("a" * 26)}, 10)
        event = [row for row in self.repo.audit_events(10) if row["id"] == audit_id][0]
        self.assertEqual({"redacted": True, "reason": "secret-pattern-detected"}, event["evidence"])
        # Opaque references may cross a controlled execution boundary; values may not.
        self.assertFalse(__import__("agent_layer.security", fromlist=["contains_secret"]).contains_secret(
            {"credential_ref": "vault://provider/key-1"}
        ))

    def test_provider_gate_and_safe_structured_success(self):
        self.authorize("critical_path.analyze")
        payload, output = self.reasoning_case("critical_path.analyze")
        provider = DeterministicProvider(
            "approved", "v1", lambda request: output
        )
        self.runtime.providers.register(provider)
        item = self.protected_item("task:1", {"due": "later"})
        invocation = Invocation(
            "critical_path.analyze", self.alice, "provider-1",
            payload, provider_id="approved", payload_item=self.protected_payload(payload),
            protected_context=(item,), evidence_refs=("task:1",),
        )
        denied = self.runtime.governed_invoke(invocation)
        self.assertEqual("PENDING_PROVIDER_UNAVAILABLE", denied.code)
        self.install_provider("approved")
        result = self.runtime.governed_invoke(invocation)
        self.assertEqual("COMPLETED", result.status)
        self.assertEqual("low", result.outcome["risk"]["level"])

    def test_incompatible_provider_training_or_data_class_fails_closed(self):
        self.authorize("market.analyze")
        self.runtime.providers.register(DeterministicProvider("bad", "v1", lambda request: {"result": "x"}))
        self.install_provider("bad", classes=("SD4",), training=True, retention="broad")
        payload, _ = self.reasoning_case("market.analyze")
        item = self.protected_item("runtime", "client fact")
        result = self.runtime.governed_invoke(Invocation(
            "market.analyze", self.alice, "bad-provider",
            payload, provider_id="bad", payload_item=self.protected_payload(payload),
            protected_context=(item,)
        ))
        self.assertEqual("PENDING_PROVIDER_UNAVAILABLE", result.code)
        self.assertIsNone(self.repo.execution("bad-provider", 10, "A"))

    def test_provider_registry_fallback_validates_attribution_and_output(self):
        registry = ProviderRegistry()
        registry.register(DeterministicProvider("first", "1", lambda request: {"wrong": True}))
        registry.register(DeterministicProvider("second", "2", lambda request: {"result": "ok"}))
        result = registry.invoke(ProviderRequest("reason", {}, {"result": str}), ["first", "second"])
        self.assertEqual("second", result.provider_id)

    def test_runtime_provider_fallback_preserves_data_policy_and_durable_state(self):
        self.authorize("provider.reason")

        def fail(request):
            raise TimeoutError("controlled failure")

        self.runtime.providers.register(DeterministicProvider("primary", "1", fail))
        payload, output = self.reasoning_case("provider.reason")
        self.runtime.providers.register(DeterministicProvider("fallback", "2", lambda request: output))
        for provider_id in ("primary", "fallback"):
            self.install_provider(provider_id)
        learning = self.repo.persist_learning(
            "provider-independent", "client", ["obs"], {"finding": "durable"},
            {"provider": "prior"}, "TEST-RETENTION", client_id=10,
        )
        item = self.protected_item("task", "scoped")
        result = self.runtime.governed_invoke(Invocation(
            "provider.reason", self.alice, "provider-fallback",
            payload, provider_id="primary", fallback_provider_ids=("fallback",),
            payload_item=self.protected_payload(payload), protected_context=(item,),
        ))
        self.assertEqual("COMPLETED", result.status)
        self.assertEqual("bounded", result.outcome["analysis"]["status"])
        self.assertEqual("fallback", self.repo.execution("provider-fallback", 10, "A")["provider_id"])
        self.assertEqual("durable", self.repo.learning(
            "provider-independent", client_id=10
        )[0]["finding"]["finding"])

        # A fallback with a broader retention mode is not eligible.
        self.runtime.providers.register(DeterministicProvider("broad", "1", lambda request: output))
        self.install_provider("broad", retention="provider-default-retention", retention_max=3600)
        denied = self.runtime.governed_invoke(Invocation(
            "provider.reason", self.alice, "provider-broad-fallback",
            payload, provider_id="primary", fallback_provider_ids=("broad",),
            payload_item=self.protected_payload(payload), protected_context=(item,),
        ))
        self.assertEqual("PENDING_PROVIDER_UNAVAILABLE", denied.code)

    def test_reasoning_capability_families_share_governance_without_shared_authority(self):
        provider = DeterministicProvider("reasoner", "fixture-1", lambda request:
                                         self.reasoning_case(request.operation)[1])
        self.runtime.providers.register(provider)
        self.install_provider("reasoner", classes=("SD3", "SD4"))
        capability_ids = (
            "flo.industry.reason", "flo.client.reason", "manager_pa.assist",
            "guardian.diagnose", "capacity.assess", "consequence.analyze",
            "performance.analyze", "market.analyze",
        )
        for capability_id in capability_ids:
            self.authorize(capability_id, entitlement=(capability_id == "manager_pa.assist"))
            context = self.protected_item("evidence:%s" % capability_id, {"fact": "scoped"})
            payload, _ = self.reasoning_case(capability_id)
            result = self.runtime.governed_invoke(Invocation(
                capability_id, self.alice, "reason:%s" % capability_id,
                payload, provider_id="reasoner", payload_item=self.protected_payload(payload),
                protected_context=(context,),
            ))
            self.assertEqual("COMPLETED", result.status, capability_id)
        # The same Client-Flo capability and provider cannot consume Client A
        # context under a Client B principal.
        self.authorize("flo.client.reason", principal=self.bob)
        leaked = self.protected_item("client-a", "private")
        payload, _ = self.reasoning_case("flo.client.reason")
        denied = self.runtime.governed_invoke(Invocation(
            "flo.client.reason", self.bob, "flo-leak",
            payload, provider_id="reasoner",
            payload_item=self.protected_payload(payload, 20, "B"), protected_context=(leaked,),
        ))
        self.assertEqual("PENDING_PROVIDER_UNAVAILABLE", denied.code)

    def test_no_agent_and_restore_security_are_deterministic(self):
        self.assertEqual("CONTINUE_DETERMINISTIC", self.runtime.no_agent_status("noagent.continuity")["status"])
        self.assertEqual("PENDING_OR_STOP", self.runtime.no_agent_status("guardian.diagnose")["status"])
        allowed = self.runtime.validate_restore_manifest(self.alice, {
            "client_id": 10, "project_code": "A", "record_classes": ["objectives", "intelligence"]
        })
        self.assertEqual("ALLOW_DATA_RESTORE", allowed["decision"])
        denied = self.runtime.validate_restore_manifest(self.alice, {
            "client_id": 10, "project_code": "A", "record_classes": ["authority_grants"]
        })
        self.assertEqual("CURRENT_AUTHORITY_REVALIDATION_REQUIRED", denied["reason"])
        self.assertEqual("DENY", self.runtime.validate_restore_manifest(
            self.alice, {"client_id": 20, "record_classes": ["objectives"]}
        )["decision"])

    def test_scoped_cache_embedding_and_index_cannot_cross_client(self):
        for kind in ("cache", "embedding", "index"):
            self.repo.store_context_artifact(
                "%s-a" % kind, kind, 10, "SD3", {"vector_or_value": [1, 2]},
                {"source_ref": "task:1"}, "TEST-RETENTION", project_code="A",
            )
            self.assertEqual(1, len(self.repo.context_artifacts(kind, 10, "A")))
            self.assertEqual([], self.repo.context_artifacts(kind, 20, "B"))
        with self.assertRaises(SecurityError):
            self.repo.store_context_artifact(
                "secret-cache", "cache", 10, "SD2", "value", {}, "TEST-RETENTION"
            )


class GovernedInvocationTests(AgentLayerFixture):
    def test_disabled_unauthorized_unhealthy_and_autonomy_denials(self):
        invocation = Invocation("help.discover", self.alice, "deny-1", {})
        self.assertEqual("CAPABILITY_DISABLED", self.runtime.governed_invoke(invocation).code)
        self.repo.set_capability_state("help.discover", self.authority_service, enabled=True)
        self.assertEqual("UNAUTHORIZED", self.runtime.governed_invoke(invocation).code)
        self.authorize("help.discover", autonomy=2)
        self.repo.set_capability_state("help.discover", self.authority_service, healthy=False, reason="fixture")
        self.assertEqual("CAPABILITY_UNHEALTHY", self.runtime.governed_invoke(invocation).code)
        self.repo.set_capability_state("help.discover", self.authority_service, healthy=True)
        too_high = Invocation("help.discover", self.alice, "deny-2", {}, requested_autonomy=3)
        self.assertEqual("AUTONOMY_CEILING", self.runtime.governed_invoke(too_high).code)

    def test_help_discovers_only_current_enabled_authorized_capabilities(self):
        self.authorize("help.discover")
        self.authorize("entitlement.evaluate")
        visible = {item["capability_id"] for item in self.runtime.discover_functions(self.alice)}
        self.assertEqual({"help.discover", "entitlement.evaluate"}, visible)
        self.repo.revoke_grant("grant:alice:entitlement.evaluate", self.owner, 10, "A")
        visible = {item["capability_id"] for item in self.runtime.discover_functions(self.alice)}
        self.assertEqual({"help.discover"}, visible)

    def test_platform_service_context_is_supported_without_inventing_tenant(self):
        platform = Principal("platform-health", "service", Scope())
        current = self.repo.authority_value("AB-AUTH-001", ["platform"])["value"]
        current["principals"]["platform-health"] = self._actor("service", "health", ())
        self.repo.install_authority_value(
            "AB-AUTH-001", "platform", current, "TEST-PLATFORM-HEALTH",
            "signed-fixture-loader", dt.datetime.utcnow(),
            proof_ref="fixture://signed/platform-health")
        self.authorize("noagent.continuity", principal=platform, domains=())
        result = self.runtime.governed_invoke(Invocation(
            "noagent.continuity", platform, "platform-continuity",
            {"requested_operation": "status"}
        ))
        self.assertEqual("COMPLETED", result.status)
        self.assertIsNone(self.repo.execution("platform-continuity")["client_id"])

    def test_entitlement_never_creates_operational_authority(self):
        self.repo.set_capability_state("manager_pa.assist", self.authority_service, enabled=True)
        self.repo.assign_entitlement("paid", 10, "capability", "manager_pa.assist",
                                     {"enabled": True}, 1, "TEST-ENT", "alice",
                                     granting_actor=self.authority_service)
        result = self.runtime.governed_invoke(Invocation(
            "manager_pa.assist", self.alice, "paid-no-authority", {}, provider_id="none"
        ))
        self.assertEqual("UNAUTHORIZED", result.code)

    def test_exactly_once_duplicate_and_concurrent_claim(self):
        self.authorize("stage2.invoke", autonomy=3)
        calls = []
        lock = threading.Lock()

        def handler(payload):
            with lock:
                calls.append(payload["structured_input"]["value"])
            return {"status": "APPLIED", "authoritative_evidence": {"table": "fixture", "id": 7}}

        self.runtime.register_handler("stage2.invoke", handler, lambda invocation: {"state": "unknown"})
        invocation = Invocation("stage2.invoke", self.alice, "exactly-once", {"stage2_capability": "fixture.apply", "structured_input": {"value": "x"}},
                                requested_autonomy=3, approval_key="approval-exact")
        self.approve(invocation)
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda _: self.runtime.governed_invoke(invocation), range(4)))
        self.assertEqual(1, len(calls))
        self.assertTrue(all(result.status in ("COMPLETED", "PENDING") for result in results))
        replay = self.runtime.governed_invoke(invocation)
        self.assertEqual("DUPLICATE_REPLAY", replay.code)
        self.assertEqual(1, len(calls))

    def test_uncertain_consequence_is_not_repeated_and_inspects_authoritative_evidence(self):
        self.authorize("stage2.invoke", autonomy=3)
        calls = []

        def uncertain(payload):
            calls.append(1)
            raise TimeoutError("transport result unknown")

        self.runtime.register_handler("stage2.invoke", uncertain, lambda invocation: {
            "state": "completed", "outcome": {"status": "APPLIED", "authoritative_evidence": {"table": "fixture", "id": 9}},
            "authoritative_evidence": {"table": "fixture", "id": 9},
        })
        invocation = Invocation("stage2.invoke", self.alice, "uncertain", {"stage2_capability": "fixture.apply", "structured_input": {"value": "x"}},
                                requested_autonomy=3, approval_key="approval-uncertain")
        self.approve(invocation)
        first = self.runtime.governed_invoke(invocation)
        self.assertEqual("OUTCOME_UNCERTAIN", first.code)
        second = self.runtime.governed_invoke(invocation)
        self.assertEqual("PRIOR_COMPLETION_ESTABLISHED", second.code)
        self.assertEqual(1, len(calls))

    def test_unknown_uncertain_outcome_escalates_without_retry(self):
        self.authorize("stage2.invoke", autonomy=3)
        calls = []

        def uncertain(payload):
            calls.append(1)
            raise TimeoutError()

        self.runtime.register_handler("stage2.invoke", uncertain, lambda invocation: {"state": "unknown"})
        invocation = Invocation("stage2.invoke", self.alice, "unknown", {"stage2_capability": "fixture.apply", "structured_input": {"value": "x"}},
                                requested_autonomy=3, approval_key="approval-unknown")
        self.approve(invocation)
        self.runtime.governed_invoke(invocation)
        result = self.runtime.governed_invoke(invocation)
        self.assertEqual("OUTCOME_CANNOT_BE_ESTABLISHED", result.code)
        self.assertEqual(1, len(calls))

    def test_approval_binds_scope_parameters_and_self_approval_is_denied(self):
        self.authorize("stage2.invoke", autonomy=3)
        invocation = Invocation("stage2.invoke", self.alice, "approval-bound", {"stage2_capability": "fixture.apply", "structured_input": {"value": "x"}},
                                requested_autonomy=3, approval_key="approval-bound")
        with self.assertRaisesRegex(Exception, "lacks current action.approve"):
            self.approve(invocation, approver=self.alice)
        changed = Invocation("stage2.invoke", self.alice, "approval-changed", {"stage2_capability": "fixture.apply", "structured_input": {"value": "changed"}},
                             requested_autonomy=3, approval_key="approval-bound")
        self.assertEqual("APPROVAL_REQUIRED", self.runtime.governed_invoke(changed).code)

    def test_r4_requires_independent_verification_and_current_revalidation(self):
        self.authorize("security.contain", autonomy=4)
        calls = []
        self.repo.security_event("contain-event", "fixture", "high", "SD3", {},
                                 client_id=10, project_code="A")
        self.runtime.register_handler("security.contain", lambda payload: {
            "status": "CONTAINED", "security_event": {"event_key": "contain-event"}, "authoritative_evidence": {"control": "disabled"}
        })
        invocation = Invocation("security.contain", self.alice, "contain",
                                {"event_key": "contain-event", "actions": {"provider_disabled": True}},
                                requested_autonomy=4, approval_key="contain-approval")
        self.approve(invocation)
        inconclusive = self.runtime.governed_invoke(invocation)
        self.assertEqual("VERIFICATION_INCONCLUSIVE", inconclusive.code)
        second = Invocation("security.contain", self.alice, "contain-2",
                            {"event_key": "contain-event", "actions": {"provider_disabled": True}},
                            requested_autonomy=4, approval_key="contain-approval-2")
        self.approve(second)
        self.runtime.register_verifier("security.contain", lambda item: {
            "result": "PASS", "verifier_principal": Principal(
                "security-verifier", "service", Scope(client_id=10, project_code="A")),
            "verifier_kind": "deterministic", "evidence": {"policy": "TEST"},
        })
        self.runtime.register_handler("security.contain", lambda payload: (
            self.repo.revoke_grant("grant:alice:security.contain", "security-verifier", 10, "A")
            or {"status": "CONTAINED", "security_event": {"event_key": "contain-event"}, "authoritative_evidence": {"control": "disabled"}}
        ))
        # Handler is never reached after a pre-handler revocation in ordinary flow;
        # explicitly revoke now to prove current authority is checked.
        self.repo.revoke_grant("grant:alice:security.contain", self.owner, 10, "A")
        self.assertEqual("UNAUTHORIZED", self.runtime.governed_invoke(second).code)

    def test_governed_configuration_commit_uses_claim_approval_verification_and_version(self):
        proposal = self.repo.propose_configuration(
            "client:10", {"disabled_capabilities": ["market.analyze"]},
            "alice", "cfg-governed-proposal", 10, "A",
        )
        self.authorize("configuration.commit", autonomy=4)
        self.runtime.register_verifier("configuration.commit", lambda item: {
            "result": "PASS", "verifier_principal": Principal(
                "config-verifier", "service", Scope(client_id=10, project_code="A")),
            "verifier_kind": "deterministic", "evidence": {"proposal_version": 1},
        })

        invocation = Invocation(
            "configuration.commit", self.alice, "cfg-governed-commit",
            {"subject_key": "client:10", "version": proposal["version"],
             "expected_effective_version": None},
            requested_autonomy=4, approval_key="cfg-governed-approval",
        )
        self.approve(invocation)
        result = self.runtime.governed_invoke(invocation)
        self.assertEqual("COMPLETED", result.status)
        self.assertEqual("effective", result.outcome["configuration"]["status"])
        self.assertEqual("DUPLICATE_REPLAY", self.runtime.governed_invoke(invocation).code)

    def test_revocation_between_claim_and_consequence_stops_before_handler(self):
        self.authorize("stage2.invoke", autonomy=3)
        calls = []
        invocation = Invocation(
            "stage2.invoke", self.alice, "revoked-after-claim", {"stage2_capability": "fixture.apply", "structured_input": {"value": "x"}},
            requested_autonomy=3, approval_key="revoked-after-claim-approval",
        )
        self.approve(invocation)
        original_claim = self.repo.claim_execution

        def claim_then_revoke(*args, **kwargs):
            result = original_claim(*args, **kwargs)
            self.repo.revoke_grant("grant:alice:stage2.invoke", self.owner, 10, "A")
            return result

        self.repo.claim_execution = claim_then_revoke
        self.runtime.register_handler("stage2.invoke", lambda payload: calls.append(payload) or {
            "status": "APPLIED", "authoritative_evidence": {"id": 1}
        })
        result = self.runtime.governed_invoke(invocation)
        self.assertEqual("AUTHORITY_REVOKED_AFTER_CLAIM", result.code)
        self.assertEqual([], calls)


class DurableStateAndIntelligenceTests(AgentLayerFixture):
    def test_objective_persists_transitions_and_scope_does_not_broaden(self):
        objective = self.repo.create_objective(
            "obj-1", "alice", 10, "Finish governed review", {"all_checks": True},
            project_code="A", dependencies=("stage2:task:1",), evidence_refs=("task:1",),
        )
        self.assertEqual("pending", objective["status"])
        active = self.repo.transition_objective("obj-1", 1, "active", "alice", 10, "A")
        self.assertEqual(2, active["version"])
        restarted = AgentRuntime(self.repo, runtime_id="restart")
        self.assertEqual("active", restarted.repository.objective("obj-1", 10, "A")["status"])
        self.assertIsNone(restarted.repository.objective("obj-1", 20, "B"))
        cancelled = self.repo.transition_objective("obj-1", 2, "cancelled", "alice", 10, "A")
        self.assertEqual("cancelled", cancelled["status"])

    def test_dependency_checkpoint_failure_localization_and_fan_out_fan_in(self):
        self.repo.create_objective("graph", "alice", 10, "Combine analysis", {"joined": True}, project_code="A")
        for key in ("branch-a", "branch-b"):
            self.repo.create_work_unit(key, "graph", 10, "critical_path.analyze", "alice",
                                       {"branch": key}, project_code="A")
        self.repo.create_work_unit("join", "graph", 10, "graph.execute", "alice", {},
                                   project_code="A", dependencies=("branch-a", "branch-b"))
        self.assertEqual("dependency_pending", self.repo.claim_work("join", "worker", 10, "A")["status"])
        claim_a = self.repo.claim_work("branch-a", "worker-a", 10, "A")
        claim_b = self.repo.claim_work("branch-b", "worker-b", 10, "A")
        self.repo.complete_work("branch-a", claim_a["claim_token"], {"result": "a"}, 10, "A")
        self.assertEqual("INCOMPLETE", self.runtime.fan_in("graph", self.alice, ["branch-a", "branch-b"])["status"])
        self.repo.complete_work("branch-b", claim_b["claim_token"], {"result": "b"}, 10, "A")
        joined = self.runtime.fan_in("graph", self.alice, ["branch-a", "branch-b"])
        self.assertEqual("COMPLETED", joined["status"])
        self.assertEqual([{"result": "a"}, {"result": "b"}], joined["artifacts"])
        join_claim = self.repo.claim_work("join", "joiner", 10, "A")
        self.assertEqual("claimed", join_claim["status"])
        self.repo.complete_work("join", join_claim["claim_token"], joined, 10, "A")
        self.assertEqual("already_completed", self.repo.claim_work("join", "other", 10, "A")["status"])

    def test_failed_branch_stays_local_and_join_does_not_false_complete(self):
        self.repo.create_objective("failure", "alice", 10, "Local failure", {"done": True}, project_code="A")
        self.repo.create_work_unit("good", "failure", 10, "critical_path.analyze", "alice", {}, project_code="A")
        self.repo.create_work_unit("bad", "failure", 10, "critical_path.analyze", "alice", {},
                                   project_code="A", failure_policy="STOP")
        good = self.repo.claim_work("good", "one", 10, "A")
        bad = self.repo.claim_work("bad", "two", 10, "A")
        self.repo.complete_work("good", good["claim_token"], {"ok": True}, 10, "A")
        self.repo.fail_work("bad", bad["claim_token"], "PROVIDER_FAILURE", 10, "A")
        joined = self.runtime.fan_in("failure", self.alice, ["good", "bad"])
        self.assertEqual("INCOMPLETE", joined["status"])
        self.assertEqual(["bad"], joined["failed"])

    def test_four_learning_scopes_and_client_user_industry_isolation(self):
        for family in ("AB-AUTH-018", "SDIP-AV-005"):
            self.repo.install_authority_value(
                family, "client:10", {"individual_persistence": True},
                "TEST-INDIVIDUAL-LEARNING", "signed-fixture-loader", dt.datetime.utcnow(),
                proof_ref="fixture://signed/individual-learning")
        records = [
            self.repo.persist_learning("platform", "platform", ["obs:p"], {"pattern": "p"}, {}, "TEST-RETENTION"),
            self.repo.persist_learning("industry", "industry", ["obs:i"], {"term": "lookahead"}, {}, "TEST-RETENTION", industry_key="construction"),
            self.repo.persist_learning("client", "client", ["obs:c"], {"pattern": "private"}, {}, "TEST-RETENTION", client_id=10),
            self.repo.persist_learning("user", "individual", ["obs:u"], {"preference": "compact"}, {}, "TEST-RETENTION", client_id=10, user_id="alice"),
        ]
        self.assertEqual({"platform", "industry", "client", "individual"},
                         {row["learning_scope"] for row in records})
        self.assertEqual([], self.repo.learning("client", client_id=20))
        self.assertEqual([], self.repo.learning("user", client_id=10, user_id="other"))
        self.assertEqual([], self.repo.learning("industry", industry_key="healthcare"))

    def test_intelligence_is_durable_versioned_scoped_and_not_stage2_truth(self):
        before_tasks = storage.SessionLocal().query(storage.Task).count()
        first = self.repo.persist_intelligence(
            "intel-1", "critical_path", ["stage2:task:1"],
            {"finding": "possible delay", "classification": "inference"},
            {"evidence_class": "model_inference", "capability": "critical_path.analyze"},
            "TEST-RETENTION", client_id=10, project_code="A", confidence={"value": 0.6},
        )
        second = self.repo.persist_intelligence(
            "intel-1", "critical_path", ["stage2:task:1", "outcome:1"],
            {"finding": "delay confirmed", "classification": "validated"},
            {"evidence_class": "historical_outcome"}, "TEST-RETENTION",
            client_id=10, project_code="A", validation={"result": "confirmed"},
            novelty_status="established", supersedes_id=first["id"],
        )
        self.assertEqual(2, second["version"])
        self.assertEqual([], self.repo.intelligence("intel-1", 20, "B"))
        self.assertEqual(1, len(self.repo.intelligence("intel-1", 10, "A")))
        with storage.SessionLocal() as session:
            self.assertEqual(before_tasks, session.query(storage.Task).count())

    def test_awareness_supervision_distribution_and_entitlement_are_separate(self):
        intel = self.repo.persist_intelligence(
            "novel", "anomaly", ["telemetry:1"], {"finding": "new"},
            {"evidence_class": "observation"}, "TEST-RETENTION", client_id=10,
            project_code="A",
        )
        current = self.repo.authority_value("AB-AUTH-001", ["client:10/project:A"])["value"]
        current["principals"]["alice"]["permissions"] = [
            "information.awareness", "information.use", "information.receive"]
        self.repo.install_authority_value(
            "AB-AUTH-001", "client:10/project:A", current,
            "TEST-RECIPIENT-AUTHORITY", "signed-fixture-loader", dt.datetime.utcnow(),
            proof_ref="fixture://signed/recipient-authority")
        aware = self.runtime.distribution_decision(intel, "alice", "awareness")
        recipient = self.runtime.distribution_decision(intel, "alice", "recipient", "email")
        unauthorized_supervisor = self.runtime.distribution_decision(intel, "bob", "supervisory")
        self.assertEqual("ALLOW-AWARENESS", aware["decision"])
        self.assertEqual("HOLD/QUARANTINE", recipient["decision"])
        self.assertEqual("DENY", unauthorized_supervisor["decision"])
        established = dict(intel, novelty_status="established")
        self.assertEqual("DENY", self.runtime.distribution_decision(
            established, "alice", "recipient", "email"
        )["decision"])
        proposal = self.repo.propose_configuration(
            "project:10:A", {"distribution": {"enabled": True,
                "recipients": ["alice"], "channels": ["email"],
                "confidentiality": ["restricted"], "intelligence_types": ["anomaly"],
                "entitlement_required": True, "entitlement_subject": "anomaly"}},
            "owner", "distribution-config", 10, "A")
        self.repo.commit_configuration("project:10:A", proposal["version"], None,
                                       "owner", "TEST-DISTRIBUTION", 10, "A")
        self.repo.assign_entitlement(
            "recipient-alice-anomaly", 10, "recipient", "anomaly", {"enabled": True},
            1, "TEST-COMMERCIAL-AUTHORITY", "alice", granting_actor=self.authority_service)
        self.assertEqual("ALLOW-DISTRIBUTION", self.runtime.distribution_decision(
            established, "alice", "recipient", "email"
        )["decision"])

    def test_distribution_delivery_and_withdrawal_are_idempotent_and_restrict_future_use(self):
        intel = self.repo.persist_intelligence(
            "dist", "finding", ["obs"], {"value": "approved"}, {},
            "TEST-RETENTION", client_id=10, novelty_status="established",
        )
        first = self.repo.record_distribution(
            "delivery-1", intel["id"], "distribution", "alice", "delivered",
            "TEST-DISTRIBUTION", client_id=10, recipient_id="alice", channel="test"
        )
        second = self.repo.record_distribution(
            "delivery-1", intel["id"], "distribution", "alice", "delivered",
            "TEST-DISTRIBUTION", client_id=10, recipient_id="alice", channel="test"
        )
        self.assertEqual(first["id"], second["id"])
        state = self.repo.withdraw("intelligence", "dist", 10, "TEST-WITHDRAWAL")
        self.assertEqual("restricted", state["state"])
        self.assertEqual([], self.repo.intelligence("dist", 10))

    def test_cross_client_generalization_remains_unset_and_disabled(self):
        self.assertIsNone(self.repo.authority_value("SDIP-AV-004", ["platform"]))
        private = self.repo.persist_learning(
            "private", "client", ["obs"], {"client_fact": "x"}, {},
            "TEST-RETENTION", client_id=10,
        )
        self.assertEqual([], self.repo.learning("private", client_id=20))


class RTW7CorrectionTests(AgentLayerFixture):
    def test_every_capability_contract_has_enforceable_owned_metadata(self):
        required = {
            "identity", "eligible_principal_classes", "input_schema", "output_schema",
            "information_contract", "required_permission", "required_configuration",
            "execution_path", "preconditions", "idempotency", "concurrency",
            "approval_contract", "audit_contract", "failure_contract",
            "uncertain_outcome", "no_agent_behavior", "verification_contract",
            "regression_dependencies",
        }
        for capability in self.repo.capabilities():
            self.assertTrue(required.issubset(capability["contract"]), capability["capability_id"])
            self.assertNotEqual("generic-success", capability["contract"]["execution_path"])
            self.assertTrue(capability["contract"]["input_schema"], capability["capability_id"])
            self.assertTrue(capability["contract"]["output_schema"], capability["capability_id"])
            self.assertTrue(capability["contract"]["input_semantics"], capability["capability_id"])
            self.assertTrue(capability["contract"]["output_semantics"], capability["capability_id"])

    def test_material_state_capabilities_execute_through_governed_path(self):
        for capability in ("configuration.propose", "learning.persist",
                           "intelligence.persist", "entitlement.evaluate",
                           "distribution.decide"):
            self.authorize(capability)
        self.repo.assign_entitlement(
            "same-eval", 10, "feature", "reports", {"enabled": True}, 1,
            "TEST-ENT", "alice", granting_actor=self.authority_service,
        )
        proposed = self.runtime.governed_invoke(Invocation(
            "configuration.propose", self.alice, "governed-propose",
            {"subject_key": "client:10", "state": {"mode": "bounded"}},
        ))
        self.assertEqual("proposed", proposed.outcome["configuration"]["status"])
        learned = self.runtime.governed_invoke(Invocation(
            "learning.persist", self.alice, "governed-learning",
            {"learning_key": "lesson", "learning_scope": "client",
             "observation_refs": ["outcome:1"], "finding": {"pattern": "x"},
             "provenance": {"source_ref": "outcome:1"}, "retention_basis": "TEST"},
        ))
        self.assertEqual("lesson", learned.outcome["learning"]["learning_key"])
        intelligence = self.runtime.governed_invoke(Invocation(
            "intelligence.persist", self.alice, "governed-intelligence",
            {"intelligence_key": "finding", "intelligence_type": "risk",
             "source_refs": ["lesson"], "content": {"risk": "bounded"},
             "provenance": {"source_ref": "lesson"}, "retention_basis": "TEST",
             "novelty_status": "established"},
        ))
        self.assertEqual("finding", intelligence.outcome["intelligence"]["intelligence_key"])
        entitled = self.runtime.governed_invoke(Invocation(
            "entitlement.evaluate", self.alice, "governed-entitlement",
            {"dimension": "feature", "subject": "reports"},
        ))
        self.assertTrue(entitled.outcome["entitled"])
        current = self.repo.authority_value("AB-AUTH-001", ["client:10/project:A"])["value"]
        current["principals"]["alice"]["permissions"] = ["information.receive"]
        self.repo.install_authority_value(
            "AB-AUTH-001", "client:10/project:A", current,
            "TEST-GOVERNED-RECIPIENT", "signed-fixture-loader", dt.datetime.utcnow(),
            proof_ref="fixture://signed/governed-recipient")
        proposal = self.repo.propose_configuration(
            "project:10:A", {"distribution": {"enabled": True,
                "recipients": ["alice"], "channels": ["test"],
                "confidentiality": ["restricted"], "intelligence_types": ["risk"],
                "entitlement_required": True, "entitlement_subject": "risk"}},
            "owner", "governed-distribution-config", 10, "A")
        self.repo.commit_configuration("project:10:A", proposal["version"], None,
                                       "owner", "TEST-DISTRIBUTION", 10, "A")
        self.repo.assign_entitlement(
            "governed-recipient", 10, "recipient", "risk", {"enabled": True}, 1,
            "TEST-COMMERCIAL-AUTHORITY", "alice", granting_actor=self.authority_service)
        decision = self.runtime.governed_invoke(Invocation(
            "distribution.decide", self.alice, "governed-distribution",
            {"intelligence_key": "finding", "decision_kind": "recipient",
             "target_principal_id": "alice", "channel": "test"},
        ))
        self.assertEqual("ALLOW-DISTRIBUTION",
                         decision.outcome["distribution_decision"]["decision"])

    def test_same_logical_keys_are_isolated_across_client_and_project(self):
        a = self.repo.create_objective("same", "alice", 10, "A outcome", {}, "A")
        b = self.repo.create_objective("same", "bob", 20, "B outcome", {}, "B")
        self.assertNotEqual(a["desired_outcome"], b["desired_outcome"])
        self.assertEqual("A outcome", self.repo.objective("same", 10, "A")["desired_outcome"])
        ia = self.repo.persist_intelligence("same-intel", "risk", [], {"client": "A"}, {},
                                            "TEST", client_id=10, project_code="A")
        ib = self.repo.persist_intelligence("same-intel", "risk", [], {"client": "B"}, {},
                                            "TEST", client_id=20, project_code="B")
        da = self.repo.record_distribution("same-delivery", ia["id"], "distribution",
                                           "alice", "held", "TEST", 10, "A")
        db = self.repo.record_distribution("same-delivery", ib["id"], "distribution",
                                           "bob", "held", "TEST", 20, "B")
        self.assertNotEqual(da["id"], db["id"])
        self.repo.persist_learning("same-learning", "client", [], {"project": "A"}, {},
                                   "TEST", client_id=10, project_code="A")
        self.repo.persist_learning("same-learning", "client", [], {"project": "B"}, {},
                                   "TEST", client_id=10, project_code="B")
        self.assertEqual("A", self.repo.learning(
            "same-learning", client_id=10, project_code="A")[0]["finding"]["project"])
        self.assertEqual("B", self.repo.learning(
            "same-learning", client_id=10, project_code="B")[0]["finding"]["project"])
        self.repo.withdraw("intelligence", "same-intel", 10, "TEST", "A")
        self.assertEqual([], self.repo.intelligence("same-intel", 10, "A"))
        self.assertEqual("B", self.repo.intelligence(
            "same-intel", 20, "B")[0]["content"]["client"])

    def test_provider_payload_cannot_bypass_classification_or_zero_data_policy(self):
        self.authorize("provider.reason")
        payload, output = self.reasoning_case("provider.reason")
        captured = []
        self.runtime.providers.register(DeterministicProvider(
            "capture", "1", lambda request: captured.append(request) or output))
        self.install_provider("capture")
        denied = self.runtime.governed_invoke(Invocation(
            "provider.reason", self.alice, "raw-payload", payload, provider_id="capture"))
        self.assertEqual("PENDING_PROVIDER_UNAVAILABLE", denied.code)
        self.assertEqual([], captured)
        self.runtime.providers.register(DeterministicProvider("zero", "1", lambda request: output))
        self.repo.install_provider_policy(
            "zero", 1, [], False, "zero-retention", True, "TEST-AUTHORITY-MATRIX",
            granting_actor=self.authority_service, allowed_confidentiality=("restricted",),
            permitted_uses=("reason",), retention_max_seconds=0,
            access_controls=("tenant-isolation",), allowed_regions=("test-region",),
            deletion_supported=True, withdrawal_supported=True,
            allowed_distribution_uses=("internal",), terms_ref="fixture://zero",
        )
        denied = self.runtime.governed_invoke(Invocation(
            "provider.reason", self.alice, "zero-policy", payload, provider_id="zero",
            payload_item=self.protected_payload(payload)))
        self.assertEqual("PENDING_PROVIDER_UNAVAILABLE", denied.code)

    def test_provider_output_contract_is_capability_owned_and_fallback_tightens(self):
        self.authorize("provider.reason")
        valid_payload, valid_output = self.reasoning_case("provider.reason")
        observed = []
        self.runtime.providers.register(DeterministicProvider(
            "owned-contract", "1", lambda request: observed.append(request.output_contract) or valid_output))
        self.install_provider("owned-contract")
        bad_payload = dict(valid_payload, output_contract={"caller_owned": "forbidden"})
        denied = self.runtime.governed_invoke(Invocation(
            "provider.reason", self.alice, "caller-output", bad_payload,
            provider_id="owned-contract", payload_item=self.protected_payload(bad_payload)))
        self.assertEqual("INVALID_INPUT_CONTRACT", denied.code)
        result = self.runtime.governed_invoke(Invocation(
            "provider.reason", self.alice, "owned-output", valid_payload,
            provider_id="owned-contract", payload_item=self.protected_payload(valid_payload)))
        self.assertEqual("COMPLETED", result.status)
        self.assertEqual({"analysis": dict, "evidence_refs": list}, observed[0])

    def test_delegation_approval_and_verification_require_authoritative_actors(self):
        with self.assertRaises(Exception):
            self.repo.grant_authority(
                "bad-grant", "worker", "service", "help.discover", "FAKE", 10, "A",
                action_permissions=("invoke",), granting_actor="arbitrary")
        with self.assertRaisesRegex(Exception, "autonomy ceiling"):
            self.repo.grant_authority(
                "wide-grant", "worker", "service", "help.discover", "FAKE", 10, "A",
                max_autonomy=3, action_permissions=("invoke",),
                granting_actor=Principal("limited-delegator", "service", Scope()))
        self.authorize("configuration.commit", autonomy=4)
        proposal = self.repo.propose_configuration("client:10", {"mode": "x"}, "alice",
                                                   "verify-proposal", 10, "A")
        invocation = Invocation(
            "configuration.commit", self.alice, "bad-verifier",
            {"subject_key": "client:10", "version": proposal["version"],
             "expected_effective_version": None}, requested_autonomy=4,
            approval_key="bad-verifier-approval")
        self.approve(invocation)
        self.runtime.register_verifier("configuration.commit", lambda item: {
            "result": "PASS", "verifier_principal": Principal(
                "arbitrary-verifier", "service", Scope(client_id=10, project_code="A")),
            "verifier_kind": "fixture", "evidence": {"ref": "none"}})
        self.assertEqual("VERIFICATION_INCONCLUSIVE",
                         self.runtime.governed_invoke(invocation).code)

    def test_all_reasoning_contracts_execute_and_reject_semantic_output(self):
        provider = DeterministicProvider(
            "contract-provider", "1", lambda request: self.reasoning_case(request.operation)[1])
        self.runtime.providers.register(provider)
        self.install_provider("contract-provider", classes=("SD3", "SD4"))
        for capability_id in (
            "flo.industry.reason", "flo.client.reason", "takeon.propose",
            "manager_pa.assist", "guardian.diagnose", "capacity.assess",
            "critical_path.analyze", "consequence.analyze", "performance.analyze",
            "provider.reason", "market.analyze",
        ):
            self.authorize(capability_id, entitlement=capability_id == "manager_pa.assist")
            payload, _ = self.reasoning_case(capability_id)
            result = self.runtime.governed_invoke(Invocation(
                capability_id, self.alice, "contract:%s" % capability_id, payload,
                provider_id="contract-provider", payload_item=self.protected_payload(payload)))
            self.assertEqual("COMPLETED", result.status, capability_id)
        invalid_payload, invalid_output = self.reasoning_case("market.analyze")
        invalid_output = dict(invalid_output, authorizes_action=True)
        self.runtime.providers.register(DeterministicProvider("semantic-bad", "1", lambda request: invalid_output))
        self.install_provider("semantic-bad")
        denied = self.runtime.governed_invoke(Invocation(
            "market.analyze", self.alice, "semantic-output", invalid_payload,
            provider_id="semantic-bad", payload_item=self.protected_payload(invalid_payload)))
        self.assertEqual("INVALID_OUTPUT_CONTRACT", denied.code)

    def test_distribution_rejects_caller_assertions_and_unestablished_recipient(self):
        self.authorize("distribution.decide")
        self.repo.persist_intelligence(
            "authority-check", "risk", ["e:1"], {"risk": "bounded"}, {}, "TEST",
            client_id=10, project_code="A", novelty_status="established")
        asserted = self.runtime.governed_invoke(Invocation(
            "distribution.decide", self.alice, "asserted-recipient",
            {"intelligence_key": "authority-check", "decision_kind": "recipient",
             "target_principal_id": "ghost", "recipient_authorized": True,
             "distribution_configured": True, "entitled": True}))
        self.assertEqual("INVALID_INPUT_CONTRACT", asserted.code)
        resolved = self.runtime.governed_invoke(Invocation(
            "distribution.decide", self.alice, "unestablished-recipient",
            {"intelligence_key": "authority-check", "decision_kind": "recipient",
             "target_principal_id": "ghost", "channel": "email"}))
        self.assertEqual("RECIPIENT_IDENTITY_UNESTABLISHED",
                         resolved.outcome["distribution_decision"]["reason"])

    def test_individual_learning_and_cross_scope_supersession_fail_closed(self):
        self.authorize("learning.persist")
        denied = self.runtime.governed_invoke(Invocation(
            "learning.persist", self.alice, "individual-unresolved",
            {"learning_key": "preference", "learning_scope": "individual",
             "observation_refs": ["o:1"], "finding": {"value": "short"},
             "provenance": {"source": "o:1"}, "retention_basis": "TEST",
             "user_id": "alice"}))
        self.assertEqual("INVALID_OUTPUT_CONTRACT", denied.code)
        other = self.repo.persist_learning(
            "shared", "client", ["o:b"], {"client": "B"}, {}, "TEST",
            client_id=20, project_code="B")
        with self.assertRaises(SecurityError):
            self.repo.persist_learning(
                "shared", "client", ["o:a"], {"client": "A"}, {}, "TEST",
                client_id=10, project_code="A", supersedes_id=other["id"])
        other_intel = self.repo.persist_intelligence(
            "shared", "risk", ["o:b"], {"client": "B"}, {}, "TEST",
            client_id=20, project_code="B")
        with self.assertRaises(SecurityError):
            self.repo.persist_intelligence(
                "shared", "risk", ["o:a"], {"client": "A"}, {}, "TEST",
                client_id=10, project_code="A", supersedes_id=other_intel["id"])

    def test_current_principal_revocation_narrowing_and_version_replacement(self):
        self.authorize("help.discover")
        first = self.runtime.governed_invoke(Invocation(
            "help.discover", self.alice, "before-principal-revoke", {"request": "help"}))
        self.assertEqual("COMPLETED", first.status)
        self.repo.revoke_authority_value("AB-AUTH-001", "client:10/project:A", self.owner)
        denied = self.runtime.governed_invoke(Invocation(
            "help.discover", self.alice, "after-principal-revoke", {"request": "help"}))
        self.assertEqual("CURRENT_PRINCIPAL_AUTHORITY_REQUIRED", denied.code)
        replacement = self.repo.install_authority_value(
            "AB-AUTH-001", "client:10/project:A",
            {"principals": {"alice": self._actor("user", "operators-a", ())}},
            "TEST-AUTHORITY-REPLACEMENT", "signed-fixture-loader", dt.datetime.utcnow(),
            proof_ref="fixture://signed/replacement")
        self.assertGreater(replacement["version"], 1)
        restored = self.runtime.governed_invoke(Invocation(
            "help.discover", self.alice, "after-principal-replacement", {"request": "help"}))
        self.assertEqual("COMPLETED", restored.status)
        self.repo.revoke_authority_value("AB-AUTH-001", "client:10/project:A", self.owner)
        narrowed_actor = self._actor("user", "operators-a", ())
        narrowed_actor["capabilities"] = ["help.discover"]
        self.repo.install_authority_value(
            "AB-AUTH-001", "client:10/project:A", {"principals": {"alice": narrowed_actor}},
            "TEST-AUTHORITY-NARROW", "signed-fixture-loader", dt.datetime.utcnow(),
            proof_ref="fixture://signed/narrow")
        self.authorize("provider.reason")
        payload, _ = self.reasoning_case("provider.reason")
        narrowed = self.runtime.governed_invoke(Invocation(
            "provider.reason", self.alice, "narrowed", payload, provider_id="none",
            payload_item=self.protected_payload(payload)))
        self.assertEqual("CURRENT_PRINCIPAL_AUTHORITY_REQUIRED", narrowed.code)

    def test_level4_limits_shadow_and_scoped_disable_controls(self):
        limits = {"fields": {"stage2_capability": {"allowed": ["fixture.apply"]}}}
        self.authorize("stage2.invoke", autonomy=4, autonomy_limits=limits, shadow_allowed=True)
        calls = []
        self.runtime.register_handler("stage2.invoke", lambda payload: calls.append(payload) or {
            "status": "APPLIED", "authoritative_evidence": {"id": len(calls)}})
        allowed = self.runtime.governed_invoke(Invocation(
            "stage2.invoke", self.alice, "level4-allowed",
            {"stage2_capability": "fixture.apply", "structured_input": {"value": 1}},
            requested_autonomy=4))
        self.assertEqual("COMPLETED", allowed.status)
        outside = self.runtime.governed_invoke(Invocation(
            "stage2.invoke", self.alice, "level4-outside",
            {"stage2_capability": "fixture.delete", "structured_input": {"value": 1}},
            requested_autonomy=4))
        self.assertEqual("AUTONOMY_LIMIT_DENIED", outside.code)
        shadow = self.runtime.governed_invoke(Invocation(
            "stage2.invoke", self.alice, "level4-shadow",
            {"stage2_capability": "fixture.apply", "structured_input": {"value": 2}},
            requested_autonomy=4, shadow_mode=True))
        self.assertEqual("SHADOWED", shadow.code)
        self.assertEqual(1, len(calls))
        proposal = self.repo.propose_configuration(
            "project:10:A", {"disabled_agent_roles": ["user"]}, "owner",
            "disable-role", 10, "A")
        self.repo.commit_configuration("project:10:A", proposal["version"], None,
                                       "owner", "TEST-CONTROL", 10, "A")
        blocked = self.runtime.governed_invoke(Invocation(
            "stage2.invoke", self.alice, "role-disabled",
            {"stage2_capability": "fixture.apply", "structured_input": {}},
            requested_autonomy=4))
        self.assertEqual("POLICY_DISABLED", blocked.code)
        service = Principal("config-verifier", "service", Scope(10, "A"))
        self.authorize("stage2.invoke", principal=service, autonomy=4,
                       autonomy_limits=limits)
        self.assertFalse(self.runtime.effective_state(service, "stage2.invoke")["disabled"])
        self.repo.revoke_configuration("project:10:A", proposal["version"], self.owner)
        action_cfg = self.repo.propose_configuration(
            "project:10:A", {"disabled_actions": ["invoke"]}, "owner",
            "disable-action", 10, "A")
        self.repo.commit_configuration("project:10:A", action_cfg["version"], None,
                                       "owner", "TEST-CONTROL", 10, "A")
        self.assertIn("action", self.runtime.effective_state(
            self.alice, "stage2.invoke")["disable_reasons"])
        self.repo.revoke_configuration("project:10:A", action_cfg["version"], self.owner)
        risk_cfg = self.repo.propose_configuration(
            "project:10:A", {"disabled_risk_classes": ["R2"]}, "owner",
            "disable-risk", 10, "A")
        self.repo.commit_configuration("project:10:A", risk_cfg["version"], None,
                                       "owner", "TEST-CONTROL", 10, "A")
        self.assertIn("risk", self.runtime.effective_state(
            self.alice, "stage2.invoke")["disable_reasons"])

    def test_correction4_entitlement_optional_fields_are_typed_and_operational(self):
        self.authorize("entitlement.configure", autonomy=4)
        self.runtime.register_verifier("entitlement.configure", lambda item: {
            "result": "PASS", "verifier_principal": Principal(
                "config-verifier", "service", Scope(10, "A")),
            "verifier_kind": "correction4", "evidence": {"controlled": True},
        })
        valid = Invocation(
            "entitlement.configure", self.alice, "c4-entitlement-valid",
            {"operation": "assign", "entitlement_key": "c4-feature",
             "dimension": "feature", "subject": "reports", "value": {"enabled": True},
             "version": 1}, requested_autonomy=4, approval_key="c4-entitlement-approval")
        self.approve(valid)
        self.assertEqual("COMPLETED", self.runtime.governed_invoke(valid).status)
        for field, value in (("dimension", []), ("subject", {"bad": True})):
            payload = dict(valid.payload, entitlement_key="c4-bad-%s" % field, **{field: value})
            invocation = Invocation("entitlement.configure", self.alice,
                                    "c4-entitlement-bad-%s" % field, payload,
                                    requested_autonomy=4,
                                    approval_key="c4-entitlement-bad-approval-%s" % field)
            self.approve(invocation)
            self.assertEqual("INVALID_INPUT_CONTRACT",
                             self.runtime.governed_invoke(invocation).code)

    def test_correction4_learning_promotion_is_explicit_and_preserves_origin(self):
        self.authorize("learning.persist")
        principal = Principal("alice", "user", Scope(10, "A", "construction"))
        denied = self.runtime.governed_invoke(Invocation(
            "learning.persist", principal, "c4-industry-denied",
            {"learning_key": "c4-lesson", "learning_scope": "industry",
             "industry_key": "construction", "observation_refs": ["o:4"],
             "finding": {"pattern": "x"}, "provenance": {"source": "o:4"},
             "retention_basis": "TEST"}))
        self.assertEqual("INVALID_INPUT_CONTRACT", denied.code)
        source = self.repo.persist_learning(
            "c4-lesson", "client", ["o:4"], {"pattern": "x"},
            {"source": "o:4"}, "TEST", client_id=10, project_code="A")
        current = self.repo.authority_value("AB-AUTH-001", ["client:10/project:A"])["value"]
        current["principals"]["alice"]["permissions"] = ["learning.promote"]
        self.repo.install_authority_value(
            "AB-AUTH-001", "client:10/project:A", current, "C4-PROMOTION-ACTOR",
            "signed-fixture-loader", dt.datetime.utcnow(),
            proof_ref="fixture://signed/c4-promotion-actor")
        for family in ("AB-AUTH-008", "SDIP-AV-004"):
            self.repo.install_authority_value(
                family, "platform", {"promotion": True}, "C4-PROMOTION",
                "signed-fixture-loader", dt.datetime.utcnow(),
                proof_ref="fixture://signed/c4-promotion")
        target_without_membership = self._actor("user", "operators-a", ("learning.promote",))
        target_without_membership["industry_keys"] = ["construction"]
        self.repo.install_authority_value(
            "AB-AUTH-001", "platform", {"principals": {
                "alice": target_without_membership}},
            "C4-PROMOTION-TARGET", "signed-fixture-loader", dt.datetime.utcnow(),
            proof_ref="fixture://signed/c4-promotion-target")
        promoted = self.runtime.promote_learning(
            principal, source, "industry", industry_key="construction")
        self.assertEqual("industry", promoted["learning_scope"])
        self.assertEqual(10, promoted["provenance"]["origin_scope"]["client_id"])
        self.assertEqual("A", promoted["provenance"]["origin_scope"]["project_code"])

    def test_correction4_supervision_requires_explicit_confidentiality(self):
        intel = self.repo.persist_intelligence(
            "c4-restricted", "risk", ["e:4"], {"finding": "bounded"},
            {"source": "e:4"}, "TEST", client_id=10, project_code="A")
        current = self.repo.authority_value("AB-AUTH-001", ["client:10/project:A"])["value"]
        current["principals"]["alice"]["permissions"] = ["information.supervise"]
        current["principals"]["alice"]["domains"] = ["SD4"]
        current["principals"]["alice"].pop("confidentiality", None)
        self.repo.install_authority_value(
            "AB-AUTH-001", "client:10/project:A", current, "C4-SUPERVISION",
            "signed-fixture-loader", dt.datetime.utcnow(),
            proof_ref="fixture://signed/c4-supervision")
        self.assertEqual("DENY", self.runtime.distribution_decision(
            intel, "alice", "supervisory")["decision"])
        current["principals"]["alice"]["confidentiality"] = ["restricted"]
        self.repo.install_authority_value(
            "AB-AUTH-001", "client:10/project:A", current, "C4-SUPERVISION-ALLOW",
            "signed-fixture-loader", dt.datetime.utcnow(),
            proof_ref="fixture://signed/c4-supervision-allow")
        self.assertEqual("SUPERVISORY-REVIEW-ELIGIBLE", self.runtime.distribution_decision(
            intel, "alice", "supervisory")["decision"])

    def test_correction4_shadow_distribution_uses_real_decision_without_persisting(self):
        self.authorize("distribution.decide", shadow_allowed=True)
        intel = self.repo.persist_intelligence(
            "c4-shadow", "risk", ["e:shadow"], {"finding": "bounded"},
            {"source": "e:shadow"}, "TEST", client_id=10, project_code="A",
            novelty_status="established")
        payload = {"intelligence_key": "c4-shadow", "decision_kind": "recipient",
                   "target_principal_id": "ghost", "channel": "email"}
        normal = self.runtime.governed_invoke(Invocation(
            "distribution.decide", self.alice, "c4-shadow-normal", payload))
        shadow = self.runtime.governed_invoke(Invocation(
            "distribution.decide", self.alice, "c4-shadow-shadow", payload,
            shadow_mode=True))
        self.assertEqual(normal.outcome["distribution_decision"],
                         shadow.outcome["distribution_decision"])
        self.assertEqual("RECIPIENT_IDENTITY_UNESTABLISHED",
                         shadow.outcome["distribution_decision"]["reason"])
        self.assertIsNotNone(self.repo.distribution("c4-shadow-normal", 10, "A"))
        self.assertIsNone(self.repo.distribution("c4-shadow-shadow", 10, "A"))

    def test_correction5_entitlement_subtype_contract_precedes_handler(self):
        self.authorize("entitlement.configure", autonomy=4)
        incomplete = Invocation(
            "entitlement.configure", self.alice, "c5-entitlement-incomplete",
            {"operation": "assign", "entitlement_key": "c5-incomplete"},
            requested_autonomy=4, approval_key="c5-entitlement-incomplete-approval")
        self.approve(incomplete)
        self.assertEqual("INVALID_INPUT_CONTRACT",
                         self.runtime.governed_invoke(incomplete).code)

    def test_correction5_promotion_requires_current_actor_authority_and_revoke(self):
        self.authorize("learning.persist")
        principal = Principal("alice", "user", Scope(10, "A", "construction"))
        source = self.repo.persist_learning(
            "c5-lesson", "client", ["o:5"], {"pattern": "x"},
            {"source": "o:5"}, "TEST", client_id=10, project_code="A")
        for family in ("AB-AUTH-008", "SDIP-AV-004"):
            self.repo.install_authority_value(
                family, "platform", {"promotion": True}, "C5-PROMOTION",
                "signed-fixture-loader", dt.datetime.utcnow(),
                proof_ref="fixture://signed/c5-promotion")
        with self.assertRaises(Exception):
            self.runtime.promote_learning(
                Principal("unestablished", "user", Scope(10, "A")), source, "industry",
                industry_key="construction")
        with self.assertRaises(Exception):
            self.runtime.promote_learning(principal, source, "industry",
                                           industry_key="construction")
        current = self.repo.authority_value("AB-AUTH-001", ["client:10/project:A"])["value"]
        current["principals"]["alice"]["permissions"] = ["learning.promote"]
        self.repo.install_authority_value(
            "AB-AUTH-001", "client:10/project:A", current, "C5-PROMOTION-ACTOR",
            "signed-fixture-loader", dt.datetime.utcnow(),
            proof_ref="fixture://signed/c5-promotion-actor")
        self.repo.install_authority_value(
            "AB-AUTH-001", "platform", {"principals": {
                "alice": self._actor("user", "operators-a", ("learning.promote",))}},
            "C5-PROMOTION-TARGET", "signed-fixture-loader", dt.datetime.utcnow(),
            proof_ref="fixture://signed/c5-promotion-target")
        promoted = self.runtime.promote_learning(
            principal, source, "industry", industry_key="construction")
        self.assertEqual("10", str(promoted["provenance"]["origin_scope"]["client_id"]))
        self.repo.revoke_authority_value("AB-AUTH-001", "client:10/project:A", self.owner)
        with self.assertRaises(Exception):
            self.runtime.promote_learning(principal, source, "platform")

    def test_correction5_all_information_decisions_require_confidentiality(self):
        intel = self.repo.persist_intelligence(
            "c5-protected", "risk", ["e:5"], {"finding": "bounded"},
            {"source": "e:5"}, "TEST", client_id=10, project_code="A")
        current = self.repo.authority_value("AB-AUTH-001", ["client:10/project:A"])["value"]
        current["principals"]["alice"]["permissions"] = [
            "information.awareness", "information.use"]
        current["principals"]["alice"]["domains"] = ["SD4"]
        current["principals"]["alice"].pop("confidentiality", None)
        self.repo.install_authority_value(
            "AB-AUTH-001", "client:10/project:A", current, "C5-INFO-NO-CONF",
            "signed-fixture-loader", dt.datetime.utcnow(),
            proof_ref="fixture://signed/c5-info-no-conf")
        self.assertEqual("DENY", self.runtime.distribution_decision(
            intel, "alice", "awareness")["decision"])
        self.assertEqual("DENY", self.runtime.distribution_decision(
            intel, "alice", "internal_use")["decision"])
        current["principals"]["alice"]["confidentiality"] = ["internal"]
        self.repo.install_authority_value(
            "AB-AUTH-001", "client:10/project:A", current, "C5-INFO-MISMATCH",
            "signed-fixture-loader", dt.datetime.utcnow(),
            proof_ref="fixture://signed/c5-info-mismatch")
        self.assertEqual("DENY", self.runtime.distribution_decision(
            intel, "alice", "awareness")["decision"])
        current["principals"]["alice"]["confidentiality"] = ["restricted"]
        self.repo.install_authority_value(
            "AB-AUTH-001", "client:10/project:A", current, "C5-INFO-ALLOW",
            "signed-fixture-loader", dt.datetime.utcnow(),
            proof_ref="fixture://signed/c5-info-allow")
        self.assertEqual("ALLOW-AWARENESS", self.runtime.distribution_decision(
            intel, "alice", "awareness")["decision"])
        self.assertEqual("ALLOW-INTERNAL-USE", self.runtime.distribution_decision(
            intel, "alice", "internal_use")["decision"])

    def test_correction5_shadow_uses_shared_builtin_planning_and_no_mutation(self):
        self.authorize("configuration.propose", shadow_allowed=True)
        payload = {"subject_key": "client:10", "state": {"c5": "planned"}}
        normal = self.runtime.governed_invoke(Invocation(
            "configuration.propose", self.alice, "c5-config-normal", payload))
        shadow = self.runtime.governed_invoke(Invocation(
            "configuration.propose", self.alice, "c5-config-shadow", payload,
            shadow_mode=True))
        self.assertEqual(normal.outcome["status"], shadow.outcome["status"])
        self.assertEqual(payload["state"], shadow.outcome["configuration"]["state"])
        self.assertIsNone(self.repo.effective_configuration(
            ["client:10"], 10, "A")["effective"].get("c5"))

    def test_correction6_promotion_replay_and_target_scope_are_claimed(self):
        self.authorize("learning.persist")
        principal = Principal("alice", "user", Scope(10, "A", "construction"))
        source = self.repo.persist_learning(
            "c6-lesson", "client", ["o:6"], {"pattern": "x"},
            {"source": "o:6"}, "TEST", client_id=10, project_code="A")
        current = self.repo.authority_value("AB-AUTH-001", ["client:10/project:A"])["value"]
        current["principals"]["alice"]["permissions"] = ["learning.promote"]
        self.repo.install_authority_value(
            "AB-AUTH-001", "client:10/project:A", current, "C6-SOURCE",
            "signed-fixture-loader", dt.datetime.utcnow(), proof_ref="fixture://c6-source")
        self.repo.install_authority_value(
            "AB-AUTH-001", "platform", {"principals": {
                "alice": self._actor("user", "operators-a", ("learning.promote",))}},
            "C6-TARGET", "signed-fixture-loader", dt.datetime.utcnow(), proof_ref="fixture://c6-target")
        for family in ("AB-AUTH-008", "SDIP-AV-004"):
            self.repo.install_authority_value(
                family, "platform", {"promotion": True}, "C6-POLICY",
                "signed-fixture-loader", dt.datetime.utcnow(), proof_ref="fixture://c6-policy")
        first = self.runtime.promote_learning(
            principal, source, "industry", industry_key="construction",
            idempotency_key="c6-promotion-once")
        replay = self.runtime.promote_learning(
            principal, source, "industry", industry_key="construction",
            idempotency_key="c6-promotion-once")
        self.assertEqual(first["id"], replay["id"])
        self.assertEqual(1, len(self.repo.learning(
            "c6-lesson", industry_key="construction")))
        with self.assertRaises(Exception):
            self.runtime.promote_learning(
                principal, source, "industry", industry_key="unrelated")

    def test_correction6_configuration_commit_shadow_has_real_contract_path(self):
        proposal = self.repo.propose_configuration(
            "project:10:A", {"c6": "planned"}, "alice", "c6-config-proposal", 10, "A")
        self.authorize("configuration.commit", autonomy=4, shadow_allowed=True)
        invocation = Invocation(
            "configuration.commit", self.alice, "c6-config-shadow",
            {"subject_key": "project:10:A", "version": proposal["version"],
             "expected_effective_version": None}, requested_autonomy=4, shadow_mode=True)
        result = self.runtime.governed_invoke(invocation)
        self.assertEqual("SHADOWED", result.code)
        self.assertEqual("COMMITTED", result.outcome["status"])
        self.assertEqual("effective", result.outcome["configuration"]["status"])
        self.assertEqual([], self.repo.effective_configuration(
            ["project:10:A"], 10, "A")["provenance"])

    def test_correction6_shadow_builtin_families_do_not_receive_unsupported_arguments(self):
        self.authorize("entitlement.configure", autonomy=4, shadow_allowed=True)
        payload = {"operation": "assign", "entitlement_key": "c6-entitlement",
                   "dimension": "feature", "subject": "reports",
                   "value": {"enabled": True}, "version": 1}
        result = self.runtime.governed_invoke(Invocation(
            "entitlement.configure", self.alice, "c6-entitlement-shadow", payload,
            requested_autonomy=4, shadow_mode=True))
        self.assertEqual("SHADOWED", result.code)
        self.assertEqual("ASSIGNED", result.outcome["status"])
        self.assertIsNone(self.repo.entitlement(10, "feature", "reports", "alice"))

    def test_correction7_promotion_rejects_spoofed_or_unestablished_target_industry(self):
        self.authorize("learning.persist")
        source = self.repo.persist_learning(
            "c7-lesson", "client", ["o:7"], {"pattern": "x"}, {"source": "o:7"},
            "TEST", client_id=10, project_code="A")
        current = self.repo.authority_value("AB-AUTH-001", ["client:10/project:A"])["value"]
        current["principals"]["alice"]["permissions"] = ["learning.promote"]
        self.repo.install_authority_value(
            "AB-AUTH-001", "client:10/project:A", current, "C7-SOURCE",
            "signed-fixture-loader", dt.datetime.utcnow(), proof_ref="fixture://c7-source")
        target_without_membership = self._actor("user", "operators-a", ("learning.promote",))
        target_without_membership.pop("industry_keys", None)
        self.repo.install_authority_value(
            "AB-AUTH-001", "platform", {"principals": {"alice": target_without_membership}},
            "C7-TARGET", "signed-fixture-loader", dt.datetime.utcnow(), proof_ref="fixture://c7-target")
        for family in ("AB-AUTH-008", "SDIP-AV-004"):
            self.repo.install_authority_value(
                family, "platform", {"promotion": True}, "C7-POLICY",
                "signed-fixture-loader", dt.datetime.utcnow(), proof_ref="fixture://c7-policy")
        spoofed = Principal("alice", "user", Scope(10, "A", "unrelated"))
        with self.assertRaises(Exception):
            self.runtime.promote_learning(spoofed, source, "industry", "unrelated")
        with self.assertRaises(Exception):
            self.runtime.promote_learning(
                Principal("alice", "user", Scope(10, "A", "construction")),
                source, "industry", "construction")
        target = self._actor("user", "operators-a", ("learning.promote",))
        target["industry_keys"] = ["construction"]
        target_principal_authority = {"alice": target,
                                      "owner": self._actor("user", "owners", ("authority.revoke",))}
        self.repo.install_authority_value(
            "AB-AUTH-001", "platform", {"principals": target_principal_authority},
            "C7-TARGET-EXACT", "signed-fixture-loader", dt.datetime.utcnow(),
            proof_ref="fixture://c7-target-exact")
        promoted = self.runtime.promote_learning(
            Principal("alice", "user", Scope(10, "A", "construction")),
            source, "industry", "construction", "c7-promotion")
        self.assertEqual("construction", promoted["industry_key"])
        self.repo.revoke_authority_value("AB-AUTH-001", "platform", self.owner)
        with self.assertRaises(Exception):
            self.runtime.promote_learning(
                Principal("alice", "user", Scope(10, "A", "construction")),
                source, "industry", "construction", "c7-promotion-after-revoke")

    def test_correction7_shadow_configuration_requires_real_scoped_proposal(self):
        self.authorize("configuration.commit", autonomy=4, shadow_allowed=True)
        missing = {"subject_key": "project:10:A", "version": 999,
                   "expected_effective_version": None}
        normal_invocation = Invocation(
            "configuration.commit", self.alice, "c7-config-normal", missing,
            requested_autonomy=4, approval_key="c7-config-normal-approval")
        self.approve(normal_invocation)
        normal = self.runtime.governed_invoke(normal_invocation)
        shadow = self.runtime.governed_invoke(Invocation(
            "configuration.commit", self.alice, "c7-config-shadow", missing,
            requested_autonomy=4, shadow_mode=True))
        self.assertEqual(normal.code, shadow.code)
        self.assertNotEqual("SHADOWED", shadow.code)

    def test_correction7_shadow_state_negative_matches_normal(self):
        self.authorize("objective.manage", shadow_allowed=True)
        payload = {"operation": "transition", "objective_key": "missing-objective",
                   "expected_version": 1, "status": "active"}
        normal = self.runtime.governed_invoke(Invocation(
            "objective.manage", self.alice, "c7-objective-normal", payload))
        shadow = self.runtime.governed_invoke(Invocation(
            "objective.manage", self.alice, "c7-objective-shadow", payload,
            shadow_mode=True))
        self.assertEqual(normal.code, shadow.code)
        self.assertNotEqual("SHADOWED", shadow.code)

    def test_correction8_stale_promotion_fixture_requires_target_membership(self):
        self.authorize("learning.persist")
        source = self.repo.persist_learning(
            "c8-lesson", "client", ["o:8"], {"pattern": "x"}, {"source": "o:8"},
            "TEST", client_id=10, project_code="A")
        current = self.repo.authority_value("AB-AUTH-001", ["client:10/project:A"])["value"]
        current["principals"]["alice"]["permissions"] = ["learning.promote"]
        self.repo.install_authority_value(
            "AB-AUTH-001", "client:10/project:A", current, "C8-SOURCE",
            "signed-fixture-loader", dt.datetime.utcnow(), proof_ref="fixture://c8-source")
        target_without_membership = self._actor("user", "operators-a", ("learning.promote",))
        target_without_membership.pop("industry_keys", None)
        self.repo.install_authority_value(
            "AB-AUTH-001", "platform", {"principals": {"alice": target_without_membership}},
            "C8-TARGET-WITHOUT-MEMBERSHIP", "signed-fixture-loader", dt.datetime.utcnow(),
            proof_ref="fixture://c8-target-without-membership")
        for family in ("AB-AUTH-008", "SDIP-AV-004"):
            self.repo.install_authority_value(
                family, "platform", {"promotion": True}, "C8-POLICY",
                "signed-fixture-loader", dt.datetime.utcnow(), proof_ref="fixture://c8-policy")
        principal = Principal("alice", "user", Scope(10, "A", "construction"))
        with self.assertRaises(Exception):
            self.runtime.promote_learning(principal, source, "industry", "construction")

    def test_correction8_configuration_revoke_shadow_plans_without_mutation(self):
        proposal = self.repo.propose_configuration(
            "project:10:A", {"c8": "revoke"}, "alice", "c8-revoke-proposal", 10, "A")
        self.repo.commit_configuration("project:10:A", proposal["version"], None,
                                       "owner", "TEST-C8", 10, "A")
        self.authorize("configuration.commit", autonomy=4, shadow_allowed=True)
        invocation = Invocation(
            "configuration.commit", self.alice, "c8-revoke-shadow",
            {"subject_key": "project:10:A", "version": proposal["version"],
             "operation": "revoke"}, requested_autonomy=4, shadow_mode=True)
        result = self.runtime.governed_invoke(invocation)
        self.assertEqual("SHADOWED", result.code)
        self.assertEqual("REVOKED", result.outcome["status"])
        self.assertEqual("revoke", self.repo.effective_configuration(
            ["project:10:A"], 10, "A")["effective"].get("c8"))

    def test_correction8_guardian_remediate_shadow_is_contractual_non_applicability(self):
        self.authorize("guardian.remediate", autonomy=4, shadow_allowed=True)
        calls = []
        self.runtime.register_handler("guardian.remediate",
                                      lambda payload: calls.append(payload))
        result = self.runtime.governed_invoke(Invocation(
            "guardian.remediate", self.alice, "c8-guardian-shadow",
            {"remediation_action": "restart", "target": "worker", "parameters": {}},
            requested_autonomy=4, shadow_mode=True))
        self.assertEqual("SHADOWED", result.code)
        self.assertEqual("SHADOW-NOT-APPLICABLE", result.outcome["status"])
        self.assertEqual([], calls)

    def test_correction8_every_consequential_capability_declares_shadow_behavior(self):
        for definition in CAPABILITY_CATALOG:
            if definition.side_effect_class in ("S3", "S4"):
                self.assertTrue(definition.shadow_applicability, definition.capability_id)

    def test_correction9_direct_promotion_requires_both_current_policy_values(self):
        self.authorize("learning.persist")
        source = self.repo.persist_learning(
            "c9-lesson", "client", ["o:9"], {"pattern": "x"}, {"source": "o:9"},
            "TEST", client_id=10, project_code="A")
        current = self.repo.authority_value("AB-AUTH-001", ["client:10/project:A"])["value"]
        current["principals"]["alice"]["permissions"] = ["learning.promote"]
        self.repo.install_authority_value(
            "AB-AUTH-001", "client:10/project:A", current, "C9-SOURCE",
            "signed-fixture-loader", dt.datetime.utcnow(), proof_ref="fixture://c9-source")
        target = self._actor("user", "operators-a", ("learning.promote",))
        target["industry_keys"] = ["construction"]
        self.repo.install_authority_value(
            "AB-AUTH-001", "platform", {"principals": {
                "alice": target,
                "owner": self._actor("user", "owners", ("authority.revoke",))}}, "C9-TARGET",
            "signed-fixture-loader", dt.datetime.utcnow(), proof_ref="fixture://c9-target")
        principal = Principal("alice", "user", Scope(10, "A", "construction"))

        def invoke(key):
            return self.runtime.governed_invoke(Invocation(
                "learning.persist", principal, key, {
                    "operation": "promote", "learning_key": source["learning_key"],
                    "learning_scope": "industry", "observation_refs": source["observation_refs"],
                    "finding": source["finding"], "provenance": source["provenance"],
                    "retention_basis": source["retention_basis"], "industry_key": "construction",
                    "source_learning_id": source["id"]}))

        self.assertEqual("PROMOTION_POLICY_REQUIRED", invoke("c9-no-policy").code)
        self.assertEqual([], self.repo.learning("c9-lesson", industry_key="construction"))
        self.repo.install_authority_value(
            "AB-AUTH-008", "platform", {"promotion": True}, "C9-AB-ONLY",
            "signed-fixture-loader", dt.datetime.utcnow(), proof_ref="fixture://c9-ab-only")
        self.assertEqual("PROMOTION_POLICY_REQUIRED", invoke("c9-ab-only").code)
        self.repo.revoke_authority_value("AB-AUTH-008", "platform", self.owner)
        self.repo.install_authority_value(
            "SDIP-AV-004", "platform", {"promotion": True}, "C9-SDIP-ONLY",
            "signed-fixture-loader", dt.datetime.utcnow(), proof_ref="fixture://c9-sdip-only")
        self.assertEqual("PROMOTION_POLICY_REQUIRED", invoke("c9-sdip-only").code)
        self.repo.install_authority_value(
            "AB-AUTH-008", "platform", {"promotion": True}, "C9-BOTH",
            "signed-fixture-loader", dt.datetime.utcnow(), proof_ref="fixture://c9-both")
        first = invoke("c9-valid")
        replay = invoke("c9-valid")
        self.assertEqual("COMPLETED", first.status)
        self.assertEqual(first.outcome["learning"]["id"], replay.outcome["learning"]["id"])
        self.assertEqual(1, len(self.repo.learning("c9-lesson", industry_key="construction")))

    def test_correction9_security_containment_shadow_is_non_applicable(self):
        self.authorize("security.contain", autonomy=4, shadow_allowed=True)
        result = self.runtime.governed_invoke(Invocation(
            "security.contain", self.alice, "c9-security-shadow",
            {"event_key": "missing-c9-event", "actions": {"disable": True}},
            requested_autonomy=4, shadow_mode=True))
        self.assertEqual("SHADOWED", result.code)
        self.assertEqual("SHADOW-NOT-APPLICABLE", result.outcome["status"])
        self.assertNotEqual("CONTAINED", result.outcome["status"])


class ObservabilityAndRegressionGuardTests(AgentLayerFixture):
    def test_security_event_can_be_contained_but_not_suppressed_by_implicated_agent(self):
        event = self.repo.security_event(
            "incident-1", "tenant_boundary_attempt", "high", "SD3",
            {"resource_ref": "task:1", "denied": True}, implicated_principal="alice",
            client_id=10, project_code="A", containment={"capability_disabled": True},
        )
        self.assertEqual("open", event["status"])
        with self.assertRaises(Exception):
            self.repo.contain_security_event("incident-1", self.alice, {"suppress": True}, 10, "A")
        contained = self.repo.contain_security_event(
            "incident-1", self.security_owner,
            {"provider_disabled": True, "preserve_evidence": True}, 10, "A"
        )
        self.assertEqual("contained", contained["status"])

    def test_audit_visibility_is_tenant_scoped(self):
        self.repo.audit("fixture", "alice", "ALLOW", {"ref": "a"}, 10, "A")
        self.repo.audit("fixture", "bob", "ALLOW", {"ref": "b"}, 20, "B")
        self.assertEqual({10}, {row["client_id"] for row in self.repo.audit_events(10)})
        fixture_rows = [row for row in self.repo.audit_events(None, platform_view=True)
                        if row["event_type"] == "fixture"]
        self.assertEqual({10, 20}, {row["client_id"] for row in fixture_rows})

    def test_capability_and_provider_revocation_blocks_future_eligibility(self):
        self.authorize("provider.reason")
        payload, output = self.reasoning_case("provider.reason")
        self.runtime.providers.register(DeterministicProvider("p", "1", lambda request: output))
        self.install_provider("p", retention="zero")
        self.repo.revoke_provider("p")
        item = self.protected_item("r", "value")
        result = self.runtime.governed_invoke(Invocation(
            "provider.reason", self.alice, "revoked-provider",
            payload, provider_id="p", payload_item=self.protected_payload(payload),
            protected_context=(item,)
        ))
        self.assertEqual("PENDING_PROVIDER_UNAVAILABLE", result.code)
        self.repo.set_capability_state("provider.reason", self.authority_service, enabled=False)
        self.assertEqual("CAPABILITY_DISABLED", self.runtime.governed_invoke(Invocation(
            "provider.reason", self.alice, "disabled", {}, provider_id="p"
        )).code)


if __name__ == "__main__":
    unittest.main()
