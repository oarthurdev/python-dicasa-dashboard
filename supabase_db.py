import os
from supabase import create_client
import pandas as pd
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SupabaseClient:
    def __init__(self, url=None, key=None):
        self.url = url or os.getenv("VITE_SUPABASE_URL")
        self.key = key or os.getenv("VITE_SUPABASE_ANON_KEY")
        
        if not self.url or not self.key:
            raise ValueError("Supabase URL and key must be provided")
        
        try:
            self.client = create_client(self.url, self.key)
            logger.info("Supabase client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Supabase client: {str(e)}")
            raise
    
    def upsert_brokers(self, brokers_df):
        """
        Insert or update broker data in the Supabase database
        
        Args:
            brokers_df (pandas.DataFrame): DataFrame containing broker data
        """
        try:
            if brokers_df.empty:
                logger.warning("No broker data to insert")
                return
            
            logger.info(f"Upserting {len(brokers_df)} brokers to Supabase")
            
            # Convert DataFrame to list of dicts
            brokers_data = brokers_df.to_dict(orient="records")
            
            # Add updated_at timestamp
            for broker in brokers_data:
                broker["updated_at"] = datetime.now().isoformat()
            
            # Upsert data to Supabase
            result = self.client.table("brokers").upsert(brokers_data).execute()
            
            if hasattr(result, "error") and result.error:
                raise Exception(f"Supabase error: {result.error}")
            
            logger.info("Brokers upserted successfully")
            return result
        
        except Exception as e:
            logger.error(f"Failed to upsert brokers: {str(e)}")
            raise
    
    def upsert_leads(self, leads_df):
        """
        Insert or update lead data in the Supabase database
        
        Args:
            leads_df (pandas.DataFrame): DataFrame containing lead data
        """
        try:
            if leads_df.empty:
                logger.warning("No lead data to insert")
                return
            
            logger.info(f"Upserting {len(leads_df)} leads to Supabase")
            
            # Convert DataFrame to list of dicts
            leads_data = leads_df.to_dict(orient="records")
            
            # Add updated_at timestamp
            for lead in leads_data:
                lead["updated_at"] = datetime.now().isoformat()
                
                # Convert datetime objects to ISO format
                if "criado_em" in lead and lead["criado_em"] is not None:
                    lead["criado_em"] = lead["criado_em"].isoformat()
                
                if "atualizado_em" in lead and lead["atualizado_em"] is not None:
                    lead["atualizado_em"] = lead["atualizado_em"].isoformat()
            
            # Upsert data to Supabase
            result = self.client.table("leads").upsert(leads_data).execute()
            
            if hasattr(result, "error") and result.error:
                raise Exception(f"Supabase error: {result.error}")
            
            logger.info("Leads upserted successfully")
            return result
        
        except Exception as e:
            logger.error(f"Failed to upsert leads: {str(e)}")
            raise
    
    def upsert_activities(self, activities_df):
        """
        Insert or update activity data in the Supabase database
        
        Args:
            activities_df (pandas.DataFrame): DataFrame containing activity data
        """
        try:
            if activities_df.empty:
                logger.warning("No activity data to insert")
                return
            
            logger.info(f"Upserting {len(activities_df)} activities to Supabase")
            
            # Convert DataFrame to list of dicts
            activities_data = activities_df.to_dict(orient="records")
            
            # Add updated_at timestamp and convert datetime objects
            for activity in activities_data:
                activity["updated_at"] = datetime.now().isoformat()
                
                # Convert datetime objects to ISO format
                if "criado_em" in activity and activity["criado_em"] is not None:
                    activity["criado_em"] = activity["criado_em"].isoformat()
            
            # Upsert data to Supabase
            result = self.client.table("activities").upsert(activities_data).execute()
            
            if hasattr(result, "error") and result.error:
                raise Exception(f"Supabase error: {result.error}")
            
            logger.info("Activities upserted successfully")
            return result
        
        except Exception as e:
            logger.error(f"Failed to upsert activities: {str(e)}")
            raise
    
    def get_broker_points(self):
        """
        Retrieve broker points from the Supabase database
        """
        try:
            logger.info("Retrieving broker points from Supabase")
            
            result = self.client.table("broker_points").select("*").execute()
            
            if hasattr(result, "error") and result.error:
                raise Exception(f"Supabase error: {result.error}")
            
            if not result.data:
                return pd.DataFrame()
            
            return pd.DataFrame(result.data)
        
        except Exception as e:
            logger.error(f"Failed to retrieve broker points: {str(e)}")
            raise
    
    def upsert_broker_points(self, points_df):
        """
        Insert or update broker points in the Supabase database
        
        Args:
            points_df (pandas.DataFrame): DataFrame containing broker points data
        """
        try:
            if points_df.empty:
                logger.warning("No broker points data to insert")
                return
            
            logger.info(f"Upserting {len(points_df)} broker points records to Supabase")
            
            # Convert DataFrame to list of dicts
            points_data = points_df.to_dict(orient="records")
            
            # Add updated_at timestamp
            for point in points_data:
                point["updated_at"] = datetime.now().isoformat()
            
            # Upsert data to Supabase
            result = self.client.table("broker_points").upsert(points_data).execute()
            
            if hasattr(result, "error") and result.error:
                raise Exception(f"Supabase error: {result.error}")
            
            logger.info("Broker points upserted successfully")
            return result
        
        except Exception as e:
            logger.error(f"Failed to upsert broker points: {str(e)}")
            raise
