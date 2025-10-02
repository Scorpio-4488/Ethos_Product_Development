# resolver.py (Polished Version)
import pandas as pd
import psycopg2
import uuid
from psycopg2.extras import execute_values

# --- Configuration ---
# Get these from Person A (Data Lead)
DB_HOST = "localhost"
DB_NAME = "hackathon_db"
DB_USER = "postgres"
DB_PASS = "your_password" # IMPORTANT: Make sure to fill this in

def fetch_unlinked_events():
    """Connects to the DB and fetches all events into a pandas DataFrame."""
    print("Step 1: Fetching unlinked events from the database...")
    try:
        with psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS) as conn:
            sql_query = """
                SELECT event_id, event_timestamp, location_name, event_type, source_identifier
                FROM events
                WHERE entity_id IS NULL
                ORDER BY event_timestamp;
            """
            df = pd.read_sql_query(sql_query, conn)
    except psycopg2.OperationalError as e:
        print(f"DATABASE CONNECTION FAILED: {e}")
        print("Please ensure your database is running and credentials are correct.")
        return None

    df['event_timestamp'] = pd.to_datetime(df['event_timestamp'])
    print(f"-> Fetched {len(df)} events to process.")
    return df

def resolve_entities_by_proximity(df):
    """
    Finds links between identifiers and merges them into groups.
    This handles transitive links (A->B, B->C => {A, B, C}).
    """
    print("Step 2: Resolving entities using proximity logic...")
    if df is None or df.empty:
        print("-> No data to resolve.")
        return []

    # This list will hold sets of linked identifiers
    linked_groups = []
    time_window = pd.Timedelta(minutes=2)

    for location, group in df.groupby('location_name'):
        group = group.sort_values('event_timestamp').reset_index()

        for i in range(len(group)):
            event1 = group.iloc[i]
            for j in range(i + 1, len(group)):
                event2 = group.iloc[j]

                if (event2['event_timestamp'] - event1['event_timestamp']) > time_window:
                    break
                
                if event1['event_type'] != event2['event_type']:
                    id1 = event1['source_identifier']
                    id2 = event2['source_identifier']
                    
                    # --- CRITICAL MERGING LOGIC ---
                    # Find which groups (if any) these IDs already belong to
                    group1_idx, group2_idx = -1, -1
                    for k, grp in enumerate(linked_groups):
                        if id1 in grp: group1_idx = k
                        if id2 in grp: group2_idx = k
                    
                    if group1_idx != -1 and group2_idx != -1:
                        # Case 1: Both IDs are in different, existing groups. Merge them.
                        if group1_idx != group2_idx:
                            linked_groups[group1_idx].update(linked_groups[group2_idx])
                            linked_groups.pop(group2_idx)
                    elif group1_idx != -1:
                        # Case 2: Only id1 is in a group. Add id2 to it.
                        linked_groups[group1_idx].add(id2)
                    elif group2_idx != -1:
                        # Case 3: Only id2 is in a group. Add id1 to it.
                        linked_groups[group2_idx].add(id1)
                    else:
                        # Case 4: Neither ID is in a group. Create a new one.
                        linked_groups.append({id1, id2})

    print(f"-> Found {len(linked_groups)} unique entity groups.")
    return linked_groups

def create_unified_entities(linked_groups):
    """
    Creates new entity profiles in the DB for each group and
    returns a mapping from the new entity_id to the original identifiers.
    """
    print("Step 3: Creating unified entity profiles in the database...")
    if not linked_groups:
        print("-> No new entities to create.")
        return {}
    
    entity_mappings = {}
    records_to_insert = []
    for group in linked_groups:
        new_entity_id = str(uuid.uuid4())
        entity_mappings[new_entity_id] = group
        records_to_insert.append((new_entity_id, 'student')) # Assuming 'student' for MVP

    with psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS) as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                "INSERT INTO entities (entity_id, entity_type) VALUES %s",
                records_to_insert
            )
    print(f"-> Successfully created {len(records_to_insert)} new entity profiles.")
    return entity_mappings

def link_events_to_entities(entity_mappings):
    """Updates the 'events' table, linking all events to their resolved entity_id."""
    print("Step 4: Linking events to their new entity profiles...")
    if not entity_mappings:
        print("-> No links to apply.")
        return
        
    with psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS) as conn:
        with conn.cursor() as cur:
            updates = 0
            for entity_id, identifiers in entity_mappings.items():
                # Using execute_values is safer and often faster for bulk operations
                sql_update = "UPDATE events SET entity_id = %s WHERE source_identifier IN %s"
                cur.execute(sql_update, (entity_id, tuple(identifiers)))
                updates += cur.rowcount
    print(f"-> Cross-Source Linking complete. Updated {updates} event rows.")


def main():
    """Main execution flow for the entity resolution process."""
    print("--- Starting Entity Resolution and Linking Pipeline ---")
    
    # Phase 1: Entity Resolution
    events_df = fetch_unlinked_events()
    if events_df is not None:
        linked_identifier_groups = resolve_entities_by_proximity(events_df)
        
        # Phase 2: Cross-Source Linking
        entity_mappings = create_unified_entities(linked_identifier_groups)
        link_events_to_entities(entity_mappings)
        
    print("--- Pipeline Finished ---")

# This makes the script runnable from the command line
if __name__ == "__main__":
    main()