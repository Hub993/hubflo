import inspect
import pathlib
import unittest

import app
import storage
from core.conversation import CoreConversation


class AnnexAArchitectureRegression(unittest.TestCase):
    def test_core_is_industry_neutral_and_has_no_storage_execution_layer(self):
        source = pathlib.Path("core/conversation.py").read_text()
        forbidden = (
            "industries.construction",
            "ConstructionIndustryModule",
            "from storage",
            "import storage",
            "SessionLocal",
            "sqlalchemy",
        )
        for value in forbidden:
            self.assertNotIn(value, source)
        self.assertIn("IndustryModule", inspect.getsource(CoreConversation))

    def test_application_routes_to_authoritative_business_handlers(self):
        source = pathlib.Path("app.py").read_text()
        for handler in (
            "create_task(",
            "create_inspection(",
            "log_delay(",
            "create_pm_reminder(",
            "cancel_pm_reminder(",
            "create_meeting(",
            "adjust_stock(",
            "approve_task(",
            "reject_task(",
        ):
            self.assertIn(handler, source)

    def test_final_schema_contains_lifecycle_datetime_and_tenant_columns(self):
        storage.Base.metadata.drop_all(storage.ENGINE)
        storage.init_db()
        inspector = storage.inspect(storage.ENGINE)
        columns = {
            table: {column["name"] for column in inspector.get_columns(table)}
            for table in (
                "conversation_states", "users", "tasks", "inspections",
                "delay_logs", "meetings", "stock_items",
            )
        }
        self.assertTrue({
            "last_activity_at", "retired_at", "retirement_reason"
        }.issubset(columns["conversation_states"]))
        self.assertTrue({
            "timezone", "date_order", "time_format", "date_display"
        }.issubset(columns["users"]))
        for table in ("tasks", "inspections", "delay_logs", "meetings", "stock_items"):
            self.assertIn("client_id", columns[table])

    def test_required_legacy_await_resolvers_remain_present(self):
        source = pathlib.Path("app.py").read_text()
        for marker in (
            "[await:item]", "[await:quantity]", "[await:supplier]",
            "[await:delivery_date]", "[await:drop_location]",
            "[await:stock_unit]", "[await:new_stock_unit]",
            "[await:new_stock_qty]",
        ):
            self.assertIn(marker, source)


if __name__ == "__main__":
    unittest.main()
