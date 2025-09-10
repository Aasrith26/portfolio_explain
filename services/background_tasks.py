import asyncio
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from data_pipeline.daily_updater_no_redis import LiveDataPipeline
from datetime import datetime

logger = logging.getLogger(__name__)


class BackgroundTaskManager:
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self.pipeline = LiveDataPipeline()

    def start_scheduler(self):
        """Start the background task scheduler"""
        try:
            # Daily data update at 2 AM IST
            self.scheduler.add_job(
                self._daily_data_update,
                CronTrigger(hour=2, minute=0, timezone='Asia/Kolkata'),
                id='daily_data_update',
                replace_existing=True,
                max_instances=1
            )

            # Backup update at 2 PM IST (in case morning fails)
            self.scheduler.add_job(
                self._backup_data_update,
                CronTrigger(hour=14, minute=0, timezone='Asia/Kolkata'),
                id='backup_data_update',
                replace_existing=True,
                max_instances=1
            )

            # Weekly full refresh (Sundays at 1 AM)
            self.scheduler.add_job(
                self._weekly_full_refresh,
                CronTrigger(day_of_week=6, hour=1, minute=0, timezone='Asia/Kolkata'),
                id='weekly_refresh',
                replace_existing=True,
                max_instances=1
            )

            self.scheduler.start()
            logger.info("✅ Background task scheduler started")

            # Run initial update
            asyncio.create_task(self._initial_data_update())

        except Exception as e:
            logger.error(f"Failed to start scheduler: {e}")

    async def _initial_data_update(self):
        """Run initial data update on startup"""
        try:
            logger.info("🚀 Running initial data update...")
            result = await self.pipeline.update_all_assets()
            logger.info(f"✅ Initial update complete: {result['update_info']}")
        except Exception as e:
            logger.error(f"❌ Initial update failed: {e}")

    async def _daily_data_update(self):
        """Daily data update task"""
        try:
            logger.info("📅 Running scheduled daily data update...")
            result = await self.pipeline.update_all_assets()

            if result['success']:
                logger.info(f"✅ Daily update successful: {result['update_info']['assets_updated']} assets updated")
            else:
                logger.error(f"❌ Daily update had issues: {result['update_info']['errors']}")

        except Exception as e:
            logger.error(f"❌ Daily update failed: {e}")

    async def _backup_data_update(self):
        """Backup data update (only if morning update failed)"""
        try:
            # Check if morning update was successful
            last_update = self.pipeline.get_last_update_info()

            if 'timestamp' in last_update:
                last_update_time = datetime.fromisoformat(last_update['timestamp'])
                hours_since = (datetime.now() - last_update_time).total_seconds() / 3600

                if hours_since < 18:  # Less than 18 hours ago
                    logger.info("⏭️ Skipping backup update - morning update was recent")
                    return

            logger.info("🔄 Running backup data update...")
            result = await self.pipeline.update_all_assets()
            logger.info(f"✅ Backup update complete: {result['update_info']}")

        except Exception as e:
            logger.error(f"❌ Backup update failed: {e}")

    async def _weekly_full_refresh(self):
        """Weekly full data refresh"""
        try:
            logger.info("🗓️ Running weekly full refresh...")

            # Clear old cache entries
            if self.pipeline.redis_client:
                try:
                    keys = self.pipeline.redis_client.keys(f"{self.pipeline.cache_prefix}:*")
                    if keys:
                        self.pipeline.redis_client.delete(*keys)
                        logger.info(f"🧹 Cleared {len(keys)} old cache entries")
                except Exception as e:
                    logger.error(f"Error clearing cache: {e}")

            # Full data refresh
            result = await self.pipeline.update_all_assets()
            logger.info(f"✅ Weekly refresh complete: {result['update_info']}")

        except Exception as e:
            logger.error(f"❌ Weekly refresh failed: {e}")

    def stop_scheduler(self):
        """Stop the scheduler"""
        try:
            if self.scheduler.running:
                self.scheduler.shutdown()
                logger.info("🛑 Background task scheduler stopped")
        except Exception as e:
            logger.error(f"Error stopping scheduler: {e}")

    def get_job_status(self):
        """Get status of scheduled jobs"""
        jobs = []
        for job in self.scheduler.get_jobs():
            jobs.append({
                'id': job.id,
                'name': job.name,
                'next_run': job.next_run_time.isoformat() if job.next_run_time else None,
                'trigger': str(job.trigger)
            })
        return {'jobs': jobs, 'scheduler_running': self.scheduler.running}


# Global instance
task_manager = BackgroundTaskManager()
