"""
PostgreSQL Database Models and Connection Manager
"""

import os
import json
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import NullPool
import logging

Base = declarative_base()
logger = logging.getLogger(__name__)


class Account(Base):
    """Telegram Account Model"""
    __tablename__ = 'accounts'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    api_id = Column(String(255), nullable=False)
    api_hash = Column(String(255), nullable=False)
    phone = Column(String(50), unique=True, nullable=False, index=True)
    source_channel = Column(String(255), nullable=False)
    target_channels = Column(JSON, nullable=False, default=[])
    status = Column(String(50), default='Added')
    session_file = Column(String(255), nullable=False)
    session_string = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        """Convert to dictionary for compatibility with existing code"""
        return {
            'id': self.id,
            'account_name': self.name,
            'api_id': self.api_id,
            'api_hash': self.api_hash,
            'phone': self.phone,
            'source_channel': self.source_channel,
            'target_channels': self.target_channels or [],
            'status': self.status,
            'session_file': self.session_file,
            'session_string': self.session_string,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

    def __repr__(self):
        return f"<Account(phone={self.phone}, name={self.name}, status={self.status})>"


class ScheduledPost(Base):
    """Scheduled Post Model"""
    __tablename__ = 'scheduled_posts'

    id = Column(Integer, primary_key=True, autoincrement=True)
    posts_data = Column(JSON, nullable=False)  # {phone: [{channel, post_id}]}
    scheduled_datetime = Column(DateTime, nullable=False, index=True)
    status = Column(String(50), default='Pending', index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    sent_at = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)

    def to_dict(self):
        """Convert to dictionary for compatibility with existing code"""
        return {
            'id': self.id,
            'posts': self.posts_data or {},
            'datetime': self.scheduled_datetime,
            'status': self.status,
            'created': self.created_at,
            'sent_at': self.sent_at,
            'error_message': self.error_message
        }

    def __repr__(self):
        return f"<ScheduledPost(id={self.id}, status={self.status}, datetime={self.scheduled_datetime})>"


class DatabaseManager:
    """Database Connection and Operations Manager"""

    def __init__(self):
        self.engine = None
        self.Session = None
        self.connected = False
        self._setup_database()

    def _get_database_url(self):
        """Get database URL from environment or use default"""
        # Railway automatically provides DATABASE_URL
        database_url = os.environ.get('DATABASE_URL')

        if not database_url:
            # Fallback for local development
            database_url = os.environ.get('POSTGRESQL_URL',
                'postgresql://postgres:postgres@localhost:5432/telegram_forwarder')

        # Fix for Railway's postgres:// to postgresql://
        if database_url and database_url.startswith('postgres://'):
            database_url = database_url.replace('postgres://', 'postgresql://', 1)

        return database_url

    def _setup_database(self):
        """Setup database connection and create tables"""
        try:
            database_url = self._get_database_url()

            if not database_url:
                logger.error("❌ DATABASE_URL not found in environment variables!")
                logger.error("Please set DATABASE_URL in Railway environment variables")
                raise ValueError("DATABASE_URL not configured")

            logger.info(f"🔌 Connecting to PostgreSQL database...")

            # Create engine with connection pooling
            self.engine = create_engine(
                database_url,
                poolclass=NullPool,  # Railway works better with NullPool
                echo=False,
                pool_pre_ping=True,  # Verify connections before using
            )

            # Create session factory
            session_factory = sessionmaker(bind=self.engine)
            self.Session = scoped_session(session_factory)

            # Check and fix schema if needed
            self._ensure_correct_schema()

            # Create all tables
            Base.metadata.create_all(self.engine)

            self.connected = True
            logger.info("✅ PostgreSQL connected successfully!")
            logger.info(f"📊 Tables: {list(Base.metadata.tables.keys())}")

        except Exception as e:
            logger.error(f"❌ Database connection failed: {str(e)}")
            logger.error("Please check your DATABASE_URL configuration")
            raise

    def _ensure_correct_schema(self):
        """Ensure database schema is correct, drop and recreate if needed"""
        try:
            from sqlalchemy import inspect, text

            inspector = inspect(self.engine)

            # Check if scheduled_posts table exists
            if 'scheduled_posts' in inspector.get_table_names():
                columns = [col['name'] for col in inspector.get_columns('scheduled_posts')]

                # If posts_data column doesn't exist, drop the table
                if 'posts_data' not in columns:
                    logger.warning("⚠️  Scheduled posts table has old schema, dropping and recreating...")
                    with self.engine.connect() as conn:
                        conn.execute(text('DROP TABLE IF EXISTS scheduled_posts CASCADE'))
                        conn.commit()
                    logger.info("✅ Old scheduled_posts table dropped")

        except Exception as e:
            logger.warning(f"⚠️  Schema check failed (continuing): {str(e)}")

    def get_session(self):
        """Get a new database session"""
        if not self.connected or not self.Session:
            raise RuntimeError("Database not connected!")
        return self.Session()

    # ==================== ACCOUNTS OPERATIONS ====================

    def get_all_accounts(self):
        """Get all accounts as dictionaries"""
        session = self.get_session()
        try:
            accounts = session.query(Account).all()
            return [acc.to_dict() for acc in accounts]
        finally:
            session.close()

    def get_account_by_phone(self, phone):
        """Get account by phone number"""
        session = self.get_session()
        try:
            account = session.query(Account).filter_by(phone=phone).first()
            return account.to_dict() if account else None
        finally:
            session.close()

    def add_account(self, account_data):
        """Add new account"""
        session = self.get_session()
        try:
            account = Account(
                name=account_data.get('account_name', account_data['phone']),
                api_id=account_data['api_id'],
                api_hash=account_data['api_hash'],
                phone=account_data['phone'],
                source_channel=account_data['source_channel'],
                target_channels=account_data.get('target_channels', []),
                status=account_data.get('status', 'Added'),
                session_file=account_data['session_file'],
                session_string=account_data.get('session_string')
            )
            session.add(account)
            session.commit()
            logger.info(f"✅ Account added to database: {account.phone}")
            return account.to_dict()
        except Exception as e:
            session.rollback()
            logger.error(f"❌ Failed to add account: {str(e)}")
            raise
        finally:
            session.close()

    def update_account(self, phone, updates):
        """Update account by phone"""
        session = self.get_session()
        try:
            account = session.query(Account).filter_by(phone=phone).first()
            if not account:
                raise ValueError(f"Account {phone} not found")

            for key, value in updates.items():
                if key == 'account_name':
                    account.name = value
                elif hasattr(account, key):
                    setattr(account, key, value)

            account.updated_at = datetime.utcnow()
            session.commit()
            logger.info(f"✅ Account updated: {phone}")
            return account.to_dict()
        except Exception as e:
            session.rollback()
            logger.error(f"❌ Failed to update account: {str(e)}")
            raise
        finally:
            session.close()

    def delete_account(self, phone):
        """Delete account by phone"""
        session = self.get_session()
        try:
            account = session.query(Account).filter_by(phone=phone).first()
            if account:
                session.delete(account)
                session.commit()
                logger.info(f"✅ Account deleted: {phone}")
                return True
            return False
        except Exception as e:
            session.rollback()
            logger.error(f"❌ Failed to delete account: {str(e)}")
            raise
        finally:
            session.close()

    # ==================== SCHEDULED POSTS OPERATIONS ====================

    def get_all_scheduled_posts(self):
        """Get all scheduled posts"""
        session = self.get_session()
        try:
            posts = session.query(ScheduledPost).order_by(ScheduledPost.scheduled_datetime).all()
            return [post.to_dict() for post in posts]
        finally:
            session.close()

    def get_pending_posts(self):
        """Get pending scheduled posts"""
        session = self.get_session()
        try:
            posts = session.query(ScheduledPost).filter_by(status='Pending').order_by(ScheduledPost.scheduled_datetime).all()
            return [post.to_dict() for post in posts]
        finally:
            session.close()

    def add_scheduled_post(self, post_data):
        """Add new scheduled post"""
        session = self.get_session()
        try:
            post = ScheduledPost(
                posts_data=post_data['posts'],
                scheduled_datetime=post_data['datetime'],
                status=post_data.get('status', 'Pending')
            )
            session.add(post)
            session.commit()
            logger.info(f"✅ Scheduled post added: ID {post.id}")
            return post.to_dict()
        except Exception as e:
            session.rollback()
            logger.error(f"❌ Failed to add scheduled post: {str(e)}")
            raise
        finally:
            session.close()

    def update_scheduled_post(self, post_id, updates):
        """Update scheduled post"""
        session = self.get_session()
        try:
            post = session.query(ScheduledPost).filter_by(id=post_id).first()
            if not post:
                raise ValueError(f"Post {post_id} not found")

            for key, value in updates.items():
                if key == 'posts':
                    post.posts_data = value
                elif key == 'datetime':
                    post.scheduled_datetime = value
                elif hasattr(post, key):
                    setattr(post, key, value)

            post.updated_at = datetime.utcnow()
            session.commit()
            return post.to_dict()
        except Exception as e:
            session.rollback()
            logger.error(f"❌ Failed to update post: {str(e)}")
            raise
        finally:
            session.close()

    def delete_scheduled_post(self, post_id):
        """Delete scheduled post"""
        session = self.get_session()
        try:
            post = session.query(ScheduledPost).filter_by(id=post_id).first()
            if post:
                session.delete(post)
                session.commit()
                logger.info(f"✅ Scheduled post deleted: ID {post_id}")
                return True
            return False
        except Exception as e:
            session.rollback()
            logger.error(f"❌ Failed to delete post: {str(e)}")
            raise
        finally:
            session.close()

    # ==================== MIGRATION HELPERS ====================

    def migrate_from_json(self, json_file_path):
        """Migrate accounts from JSON file to PostgreSQL"""
        if not os.path.exists(json_file_path):
            logger.warning(f"JSON file not found: {json_file_path}")
            return 0

        try:
            with open(json_file_path, 'r', encoding='utf-8') as f:
                accounts_data = json.load(f)

            migrated = 0
            for account_data in accounts_data:
                try:
                    # Check if account already exists
                    existing = self.get_account_by_phone(account_data['phone'])
                    if existing:
                        logger.info(f"⚠️  Account {account_data['phone']} already exists, skipping")
                        continue

                    self.add_account(account_data)
                    migrated += 1
                except Exception as e:
                    logger.error(f"Failed to migrate account {account_data.get('phone')}: {str(e)}")

            logger.info(f"✅ Migrated {migrated} accounts from JSON to PostgreSQL")
            return migrated
        except Exception as e:
            logger.error(f"❌ Migration failed: {str(e)}")
            raise

    def close(self):
        """Close database connections"""
        if self.Session:
            self.Session.remove()
        if self.engine:
            self.engine.dispose()
        self.connected = False
        logger.info("Database connections closed")


# Global database instance
db = None

def get_db():
    """Get or create database instance"""
    global db
    if db is None:
        db = DatabaseManager()
    return db
