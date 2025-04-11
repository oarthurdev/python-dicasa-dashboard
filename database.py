import os
import pandas as pd
import logging
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DatabaseClient:
    def __init__(self, db_url=None):
        self.db_url = db_url or os.getenv("DATABASE_URL")
        
        if not self.db_url:
            raise ValueError("Database URL must be provided")
        
        try:
            self.engine = create_engine(self.db_url)
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection established successfully")
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            raise
    
    def upsert_brokers(self, brokers_df):
        """
        Insert or update broker data in the database
        
        Args:
            brokers_df (pandas.DataFrame): DataFrame containing broker data
        """
        try:
            if brokers_df.empty:
                logger.warning("No broker data to insert")
                return
            
            logger.info(f"Upserting {len(brokers_df)} brokers to database")
            
            # Add updated_at timestamp
            brokers_df['updated_at'] = datetime.now()
            
            # Use pandas to_sql with 'replace' method for simplicity
            # In a production environment, a more efficient upsert would be implemented
            brokers_df.to_sql('brokers', self.engine, if_exists='append', index=False, 
                             method='multi', chunksize=500)
            
            logger.info("Brokers upserted successfully")
        
        except Exception as e:
            logger.error(f"Failed to upsert brokers: {str(e)}")
            # Don't raise the exception, just log it and continue
    
    def upsert_leads(self, leads_df):
        """
        Insert or update lead data in the database
        
        Args:
            leads_df (pandas.DataFrame): DataFrame containing lead data
        """
        try:
            if leads_df.empty:
                logger.warning("No lead data to insert")
                return
            
            logger.info(f"Upserting {len(leads_df)} leads to database")
            
            # Add updated_at timestamp
            leads_df['updated_at'] = datetime.now()
            
            # Use pandas to_sql with 'replace' method
            leads_df.to_sql('leads', self.engine, if_exists='append', index=False,
                          method='multi', chunksize=500)
            
            logger.info("Leads upserted successfully")
        
        except Exception as e:
            logger.error(f"Failed to upsert leads: {str(e)}")
            # Don't raise the exception, just log it and continue
    
    def upsert_activities(self, activities_df):
        """
        Insert or update activity data in the database
        
        Args:
            activities_df (pandas.DataFrame): DataFrame containing activity data
        """
        try:
            if activities_df.empty:
                logger.warning("No activity data to insert")
                return
            
            logger.info(f"Upserting {len(activities_df)} activities to database")
            
            # Add updated_at timestamp
            activities_df['updated_at'] = datetime.now()
            
            # Use pandas to_sql with 'replace' method
            activities_df.to_sql('activities', self.engine, if_exists='append', index=False,
                                method='multi', chunksize=500)
            
            logger.info("Activities upserted successfully")
        
        except Exception as e:
            logger.error(f"Failed to upsert activities: {str(e)}")
            # Don't raise the exception, just log it and continue
    
    def get_broker_points(self):
        """
        Retrieve broker points from the database
        """
        try:
            logger.info("Retrieving broker points from database")
            
            query = "SELECT * FROM broker_points"
            
            df = pd.read_sql(query, self.engine)
            
            return df
        
        except Exception as e:
            logger.error(f"Failed to retrieve broker points: {str(e)}")
            return pd.DataFrame()
    
    def upsert_broker_points(self, points_df):
        """
        Insert or update broker points in the database
        
        Args:
            points_df (pandas.DataFrame): DataFrame containing broker points data
        """
        try:
            if points_df.empty:
                logger.warning("No broker points data to insert")
                return
            
            logger.info(f"Upserting {len(points_df)} broker points records to database")
            
            # Add updated_at timestamp
            points_df['updated_at'] = datetime.now()
            
            # Use pandas to_sql with 'replace' method
            points_df.to_sql('broker_points', self.engine, if_exists='append', index=False,
                            method='multi', chunksize=500)
            
            logger.info("Broker points upserted successfully")
        
        except Exception as e:
            logger.error(f"Failed to upsert broker points: {str(e)}")
            # Don't raise the exception, just log it and continue
    
    def get_cached_data(self):
        """
        Retrieve cached data from the database if available
        
        Returns:
            dict: Dictionary containing brokers, leads, activities dataframes or None if data is not available
        """
        try:
            logger.info("Checking for cached data in database")
            
            # Check if tables have data
            with self.engine.connect() as conn:
                broker_count = conn.execute(text("SELECT COUNT(*) FROM brokers")).scalar()
                leads_count = conn.execute(text("SELECT COUNT(*) FROM leads")).scalar()
                activities_count = conn.execute(text("SELECT COUNT(*) FROM activities")).scalar()
            
            if broker_count == 0 or leads_count == 0 or activities_count == 0:
                logger.info("No cached data available")
                return None
            
            logger.info(f"Found cached data: {broker_count} brokers, {leads_count} leads, {activities_count} activities")
            
            # Retrieve data
            brokers_df = pd.read_sql("SELECT * FROM brokers", self.engine)
            leads_df = pd.read_sql("SELECT * FROM leads", self.engine)
            activities_df = pd.read_sql("SELECT * FROM activities", self.engine)
            
            return {
                'brokers': brokers_df,
                'leads': leads_df,
                'activities': activities_df
            }
            
        except SQLAlchemyError as e:
            logger.error(f"Database error retrieving cached data: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Error retrieving cached data: {str(e)}")
            return None
    
    def clear_cache(self):
        """
        Clear all cached data from the database
        """
        try:
            logger.info("Clearing cached data from database")
            
            with self.engine.connect() as conn:
                conn.execute(text("DELETE FROM activities"))
                conn.execute(text("DELETE FROM broker_points"))
                conn.execute(text("DELETE FROM leads"))
                conn.execute(text("DELETE FROM brokers"))
                conn.commit()
            
            logger.info("Cache cleared successfully")
            
        except Exception as e:
            logger.error(f"Failed to clear cache: {str(e)}")