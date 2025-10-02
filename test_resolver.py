import pandas as pd
from resolver import resolve_entities_by_proximity # Import your upgraded function

print("--- Testing Upgraded Entity Resolver ---")

# --- Comprehensive Test Data ---
# This DataFrame includes multiple scenarios to test the advanced logic.
test_data = {
    'event_timestamp': [
        # Scenario 1: Path Twinning (Threshold is 0.75, so needs 2+ sightings)
        # Pair (card_path_A, wifi_path_X) - seen ONCE, score 0.5 -> SHOULD NOT be linked.
        '2025-10-02 13:00:00', '2025-10-02 13:00:15',
        # Pair (card_path_B, wifi_path_Y) - seen TWICE, score 0.75 -> SHOULD BE linked.
        '2025-10-02 14:00:00', '2025-10-02 14:00:30', # Sighting 1 in Lab_1
        '2025-10-02 15:00:00', '2025-10-02 15:01:00', # Sighting 2 in Library

        # Scenario 2: Dynamic Window (Main_Entrance has a 30s window)
        # Pair with 20s diff -> SHOULD BE linked.
        '2025-10-02 11:00:00', '2025-10-02 11:00:20',
        # Pair with 40s diff -> SHOULD NOT be linked.
        '2025-10-02 12:00:00', '2025-10-02 12:00:40',

        # Scenario 3: Transitive Linking
        # card_transitive links to wifi_transitive
        '2025-10-02 16:00:00', '2025-10-02 16:00:10',
        # wifi_transitive links to face_transitive
        '2025-10-02 17:00:00', '2025-10-02 17:00:20',
    ],
    'location_name': [
        # S1
        'Cafeteria', 'Cafeteria',
        'Lab_1', 'Lab_1',
        'Library', 'Library',
        # S2
        'Main_Entrance', 'Main_Entrance',
        'Main_Entrance', 'Main_Entrance',
        # S3
        'Admin_Block', 'Admin_Block',
        'Server_Room', 'Server_Room',
    ],
    'event_type': [
        # S1
        'card_swipe', 'wifi_log',
        'card_swipe', 'wifi_log',
        'card_swipe', 'wifi_log',
        # S2
        'card_swipe', 'face_scan',
        'card_swipe', 'wifi_log',
        # S3
        'card_swipe', 'wifi_log',
        'wifi_log', 'face_scan',
    ],
    'source_identifier': [
        # S1
        'card_path_A', 'wifi_path_X', # Will have score 0.5
        'card_path_B', 'wifi_path_Y', # Will have score 0.75
        'card_path_B', 'wifi_path_Y',
        # S2
        'card_dynamic_pass', 'face_dynamic_pass',
        'card_dynamic_fail', 'wifi_dynamic_fail',
        # S3
        'card_transitive', 'wifi_transitive',
        'wifi_transitive', 'face_transitive',
    ]
}
test_df = pd.DataFrame(test_data)
test_df['event_timestamp'] = pd.to_datetime(test_df['event_timestamp'])

# --- Execute the Function ---
# Note: your resolver.py file must have CONFIDENCE_SCORE_THRESHOLD = 0.75 for this test to work
resolved_groups = resolve_entities_by_proximity(test_df)

print("\n--- Test Results ---")
print(f"Function returned {len(resolved_groups)} groups:")
for i, group in enumerate(resolved_groups):
    # Sorting the list makes the output consistent and easier to read
    print(f"  Group {i+1}: {sorted(list(group))}")

# In test_resolver.py, replace the "Automated Verification" block

print("\n--- Automated Verification ---")
result_sets = {frozenset(g) for g in resolved_groups}

# Define the groups we expect to find with the new, simpler logic
expected_group_1 = frozenset({'card_path_B', 'wifi_path_Y'})
expected_group_2 = frozenset({'card_transitive', 'face_transitive', 'wifi_transitive'})
expected_group_3 = frozenset({'card_dynamic_pass', 'face_dynamic_pass'})
expected_group_4 = frozenset({'card_path_A', 'wifi_path_X'})

# Check if all expected groups were found
all_found = True
for i, expected_group in enumerate([expected_group_1, expected_group_2, expected_group_3, expected_group_4]):
    if expected_group in result_sets:
        print(f"✅ PASS: Test case {i+1} successful.")
    else:
        print(f"❌ FAIL: Test case {i+1} failed. Expected group {expected_group} was not found.")
        all_found = False

# Check that the out-of-window pair was not linked with anything
out_of_window_pair = {'card_dynamic_fail', 'wifi_dynamic_fail'}
was_linked = any(g.intersection(out_of_window_pair) and len(g) > 1 for g in result_sets)
if not was_linked:
    print("✅ PASS: Out-of-window pair was correctly excluded.")
else:
    print("❌ FAIL: Out-of-window pair was incorrectly included in a larger group.")