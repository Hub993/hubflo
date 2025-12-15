# seed_v6_1.py
# One-shot deterministic seed for HubFlo
# Uses MAIN storage.py (authoritative DB)

from datetime import datetime, timedelta

from storage import (
    SessionLocal,
    User,
    Task,
    PMProjectMap,
    StockItem
)

PROJECT = "P1"

def run_seed():
    db = SessionLocal()
    try:
        # -----------------
        # USERS
        # -----------------
        pm = User(
            wa_id="10000000001",
            name="PM One",
            role="pm",
            active=True,
            project_code=PROJECT,
            subcontractor_name=None
        )

        plumber = User(
            wa_id="10000000002",
            name="John Plumbing",
            role="sub",
            active=True,
            project_code=PROJECT,
            subcontractor_name="plumbing"
        )

        painter = User(
            wa_id="10000000003",
            name="Ace Painting",
            role="sub",
            active=True,
            project_code=PROJECT,
            subcontractor_name="painting"
        )

        db.add_all([pm, plumber, painter])
        db.flush()

        # -----------------
        # PM ↔ PROJECT MAP
        # -----------------
        db.add(PMProjectMap(
            pm_user_id=pm.id,
            project_code=PROJECT
        ))

        # -----------------
        # TASKS
        # -----------------
        now = datetime.utcnow()

        tasks = [
            Task(
                sender=plumber.wa_id,
                text="Fix leaking pipe in unit 2",
                status="open",
                tag="task",
                project_code=PROJECT,
                subcontractor_name="plumbing",
                order_state=None,
                subtype="assigned",
                overrun_days=2,
                last_updated=now - timedelta(days=2)
            ),
            Task(
                sender=painter.wa_id,
                text="Paint barn exterior",
                status="open",
                tag="task",
                project_code=PROJECT,
                subcontractor_name="painting",
                order_state=None,
                subtype="assigned",
                overrun_days=0,
                last_updated=now
            ),
            Task(
                sender=pm.wa_id,
                text="Install fencing section A",
                status="open",
                tag="task",
                project_code=PROJECT,
                subcontractor_name=None,
                order_state="await_item",
                subtype="self",
                overrun_days=0,
                last_updated=now
            )
        ]

        db.add_all(tasks)

        # -----------------
        # STOCK ITEMS
        # -----------------
        stock_items = [
            StockItem(
                name="Cement",
                project_code=PROJECT,
                supplier_name="Default Supplier",
                unit="bag",
                current_qty=100,
                min_days_cover=7
            ),
            StockItem(
                name="Timber",
                project_code=PROJECT,
                supplier_name="Default Supplier",
                unit="pallet",
                current_qty=20,
                min_days_cover=5
            )
        ]

        db.add_all(stock_items)

        db.commit()
        print("✅ HubFlo seed completed successfully")

    except Exception as e:
        db.rollback()
        print("❌ Seed failed:", e)
        raise
    finally:
        db.close()


if __name__ == "__main__":
    run_seed()