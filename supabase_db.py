import os
from supabase import create_client
import pandas as pd
import numpy as np
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

            # Make a copy of the DataFrame to avoid modifying the original
            leads_df_clean = leads_df.copy()

            # Replace infinite values with None (null in JSON)
            numeric_cols = leads_df_clean.select_dtypes(include=['float', 'int']).columns
            for col in numeric_cols:
                # Replace NaN and infinite values with None
                mask = ~np.isfinite(leads_df_clean[col])
                if mask.any():
                    leads_df_clean.loc[mask, col] = None

            # Convert bigint columns from float to int to avoid "invalid input syntax for type bigint" errors
            bigint_columns = ['id', 'responsavel_id', 'status_id', 'pipeline_id']
            for col in bigint_columns:
                if col in leads_df_clean.columns:
                    # Only convert finite values (NaN/None will be handled separately)
                    mask = np.isfinite(leads_df_clean[col])
                    if mask.any():
                        leads_df_clean.loc[mask, col] = leads_df_clean.loc[mask, col].astype('Int64')

            # Convert DataFrame to list of dicts
            leads_data = leads_df_clean.to_dict(orient="records")

            # Add updated_at timestamp
            for lead in leads_data:
                lead["updated_at"] = datetime.now().isoformat()

                # Convert datetime objects to ISO format
                if "criado_em" in lead and lead["criado_em"] is not None:
                    lead["criado_em"] = lead["criado_em"].isoformat()

                if "atualizado_em" in lead and lead["atualizado_em"] is not None:
                    lead["atualizado_em"] = lead["atualizado_em"].isoformat()

                # Additional check for any remaining non-JSON compatible values and type conversions
                for key, value in list(lead.items()):  # Create a list to avoid "dictionary changed size during iteration"
                    # Check for NaN, Infinity, -Infinity in float values
                    if isinstance(value, float) and (np.isnan(value) or np.isinf(value)):
                        lead[key] = None
                    # Convert float to int for bigint columns
                    elif key in bigint_columns and isinstance(value, float) and value.is_integer():
                        lead[key] = int(value)

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

            logger.info(f"Processing {len(activities_df)} activities")

            # First, get a list of all lead_ids in the leads table
            try:
                # Query existing lead IDs from the database to ensure we only insert activities for existing leads
                leads_result = self.client.table("leads").select("id").execute()
                if hasattr(leads_result, "error") and leads_result.error:
                    raise Exception(f"Supabase error querying leads: {leads_result.error}")

                # Create a set of existing lead IDs for faster lookup
                existing_lead_ids = set()
                for lead in leads_result.data:
                    existing_lead_ids.add(lead['id'])

                logger.info(f"Found {len(existing_lead_ids)} existing leads in database")
            except Exception as e:
                logger.warning(f"Could not query existing leads, proceeding without validation: {str(e)}")
                existing_lead_ids = None

            # Make a copy of the DataFrame to avoid modifying the original
            activities_df_clean = activities_df.copy()

            # Replace infinite values with None (null in JSON)
            numeric_cols = activities_df_clean.select_dtypes(include=['float', 'int']).columns
            for col in numeric_cols:
                # Replace NaN and infinite values with None
                mask = ~np.isfinite(activities_df_clean[col])
                if mask.any():
                    activities_df_clean.loc[mask, col] = None

            # Convert bigint columns from float to int to avoid "invalid input syntax for type bigint" errors
            bigint_columns = ['lead_id', 'user_id']
            for col in bigint_columns:
                if col in activities_df_clean.columns:
                    # Only convert finite values (NaN/None will be handled separately)
                    mask = np.isfinite(activities_df_clean[col])
                    if mask.any():
                        activities_df_clean.loc[mask, col] = activities_df_clean.loc[mask, col].astype('Int64')

            # The 'id' column in activities table is of type TEXT in SQL, but Kommo API might return it as a number
            # We need to ensure it's converted to string
            if 'id' in activities_df_clean.columns:
                activities_df_clean['id'] = activities_df_clean['id'].astype(str)

            # Get a list of all broker_ids in the brokers table
            try:
                # Query existing broker IDs from the database to ensure we only insert activities with valid user_ids
                brokers_result = self.client.table("brokers").select("id").execute()
                if hasattr(brokers_result, "error") and brokers_result.error:
                    raise Exception(f"Supabase error querying brokers: {brokers_result.error}")

                # Create a set of existing broker IDs for faster lookup
                existing_broker_ids = set()
                for broker in brokers_result.data:
                    existing_broker_ids.add(broker['id'])

                logger.info(f"Found {len(existing_broker_ids)} existing brokers in database")
            except Exception as e:
                logger.warning(f"Could not query existing brokers, proceeding without validation: {str(e)}")
                existing_broker_ids = None

            # Filter activities to only include those with existing lead_ids and user_ids
            filter_needed = False

            # Filter by lead_id
            if existing_lead_ids is not None and 'lead_id' in activities_df_clean.columns:
                filter_needed = True
                original_count = len(activities_df_clean)
                activities_df_clean = activities_df_clean[
                    activities_df_clean['lead_id'].isin(existing_lead_ids) | 
                    activities_df_clean['lead_id'].isna()
                ]
                filtered_count = len(activities_df_clean)
                if filtered_count < original_count:
                    logger.warning(f"Filtered out {original_count - filtered_count} activities with non-existent lead_ids")

            # Filter by user_id
            if existing_broker_ids is not None and 'user_id' in activities_df_clean.columns:
                filter_needed = True
                original_count = len(activities_df_clean)
                activities_df_clean = activities_df_clean[
                    activities_df_clean['user_id'].isin(existing_broker_ids) | 
                    activities_df_clean['user_id'].isna()
                ]
                filtered_count = len(activities_df_clean)
                if filtered_count < original_count:
                    logger.warning(f"Filtered out {original_count - filtered_count} activities with non-existent user_ids")

            # If we have no activities after filtering, exit early
            if activities_df_clean.empty:
                logger.warning("No valid activities to insert after filtering")
                return

            logger.info(f"Upserting {len(activities_df_clean)} activities to Supabase")

            # Convert DataFrame to list of dicts
            activities_data = activities_df_clean.to_dict(orient="records")

            # Add updated_at timestamp and convert datetime objects
            for activity in activities_data:
                activity["updated_at"] = datetime.now().isoformat()

                # Convert datetime objects to ISO format
                if "criado_em" in activity and activity["criado_em"] is not None:
                    activity["criado_em"] = activity["criado_em"].isoformat()

                # Additional check for any remaining non-JSON compatible values and type conversions
                for key, value in list(activity.items()):  # Create a list to avoid "dictionary changed size during iteration"
                    # Check for NaN, Infinity, -Infinity in float values
                    if isinstance(value, float) and (np.isnan(value) or np.isinf(value)):
                        activity[key] = None
                    # Convert float to int for bigint columns
                    elif key in bigint_columns and isinstance(value, float) and value.is_integer():
                        activity[key] = int(value)

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

            # Make a copy of the DataFrame to avoid modifying the original
            points_df_clean = points_df.copy()

            # Replace infinite values with None (null in JSON)
            numeric_cols = points_df_clean.select_dtypes(include=['float', 'int']).columns
            for col in numeric_cols:
                # Replace NaN and infinite values with None
                mask = ~np.isfinite(points_df_clean[col])
                if mask.any():
                    points_df_clean.loc[mask, col] = None

            # Convert bigint columns from float to int to avoid "invalid input syntax for type bigint" errors
            bigint_columns = ['id']
            for col in bigint_columns:
                if col in points_df_clean.columns:
                    # Only convert finite values (NaN/None will be handled separately)
                    mask = np.isfinite(points_df_clean[col])
                    if mask.any():
                        points_df_clean.loc[mask, col] = points_df_clean.loc[mask, col].astype('Int64')

            # Get brokers list to filter only "Corretor"
            brokers_result = self.client.table("brokers").select("id").eq("cargo", "Corretor").execute()
            if hasattr(brokers_result, "error") and brokers_result.error:
                raise Exception(f"Supabase error querying brokers: {brokers_result.error}")

            # Create set of broker IDs
            corretor_ids = {broker['id'] for broker in brokers_result.data}

            # Filter points to include only corretores
            points_df_clean = points_df_clean[points_df_clean['id'].isin(corretor_ids)]


            # Convert DataFrame to list of dicts
            points_data = points_df_clean.to_dict(orient="records")

            # Add updated_at timestamp
            for point in points_data:
                point["updated_at"] = datetime.now().isoformat()

                # Additional check for any remaining non-JSON compatible values and type conversions
                for key, value in list(point.items()):  # Create a list to avoid "dictionary changed size during iteration"
                    # Check for NaN, Infinity, -Infinity in float values
                    if isinstance(value, float) and (np.isnan(value) or np.isinf(value)):
                        point[key] = None
                    # Convert float to int for bigint columns
                    elif key in bigint_columns and isinstance(value, float) and value.is_integer():
                        point[key] = int(value)

            # Upsert data to Supabase preserving existing data
            for point in points_data:
                # Check if record exists
                existing = self.client.table("broker_points").select("*").eq("id", point["id"]).execute()

                if existing.data:
                    # Update only if new values are different
                    update_data = {k: v for k, v in point.items() if k != "id" and v is not None}
                    if update_data:
                        result = self.client.table("broker_points").update(update_data).eq("id", point["id"]).execute()
                else:
                    # Insert new record
                    result = self.client.table("broker_points").insert(point).execute()

            if hasattr(result, "error") and result.error:
                raise Exception(f"Supabase error: {result.error}")

            logger.info("Broker points upserted successfully")
            return result

        except Exception as e:
            logger.error(f"Failed to upsert broker points: {str(e)}")
            raise