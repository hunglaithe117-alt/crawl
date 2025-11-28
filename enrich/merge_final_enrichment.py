import pandas as pd
import os
import sys

def main():
    # Paths
    main_file = "/Users/hunglai/hust/20251/thesis/19314170/final-2017-01-25.800c.2y.all.builds.risk.cleaned.csv"
    enriched_file = "/Users/hunglai/hust/20251/thesis/19314170/github_enrich_continue_output/merged_results/all_enriched_data.csv"
    output_file = "/Users/hunglai/hust/20251/thesis/19314170/final-2017-01-25.800c.2y.all.builds.risk.enriched.csv"

    print(f"Loading main file: {main_file}")
    df_main = pd.read_csv(main_file)
    print(f"Main file shape: {df_main.shape}")

    print(f"Loading enriched file: {enriched_file}")
    df_enriched = pd.read_csv(enriched_file)
    print(f"Enriched file shape: {df_enriched.shape}")

    # Columns to remove
    cols_to_remove = ['author_ownership', 'file_change_frequency']
    
    # Remove from main if present
    df_main.drop(columns=[c for c in cols_to_remove if c in df_main.columns], inplace=True)
    
    # Remove from enriched if present
    df_enriched.drop(columns=[c for c in cols_to_remove if c in df_enriched.columns], inplace=True)

    # Columns to merge from enriched
    # We want to keep all columns from main, and add new ones from enriched.
    # We join on tr_build_id.
    # First, identify columns in enriched that are NOT in main (plus the join key)
    
    # Actually, let's just take the specific new features we know we want, plus the key
    new_features = [
        "gh_num_reviewers",
        "gh_num_approvals",
        "gh_time_to_first_review",
        "gh_review_sentiment",
        "gh_linked_issues_count",
        "gh_has_bug_label"
    ]
    
    # Check which of these are actually in df_enriched
    available_features = [c for c in new_features if c in df_enriched.columns]
    
    # Prepare right dataframe
    cols_to_merge = ['tr_build_id'] + available_features
    df_to_merge = df_enriched[cols_to_merge].copy()
    
    # Drop duplicates in enriched just in case
    df_to_merge.drop_duplicates(subset=['tr_build_id'], inplace=True)

    print(f"Merging {len(available_features)} features...")
    
    # Merge
    # We use left join to keep all rows from main
    # Use suffixes to handle overlapping columns
    df_merged = pd.merge(df_main, df_to_merge, on='tr_build_id', how='left', suffixes=('', '_new'))
    
    # Handle overlapping columns (merging _new into original)
    for feature in available_features:
        new_col = f"{feature}_new"
        if new_col in df_merged.columns:
            print(f"Resolving overlap for {feature}...")
            # Use the new value if available, otherwise keep the old value
            df_merged[feature] = df_merged[new_col].combine_first(df_merged[feature])
            df_merged.drop(columns=[new_col], inplace=True)
    
    print(f"Merged shape: {df_merged.shape}")
    
    # Save
    print(f"Saving to {output_file}")
    df_merged.to_csv(output_file, index=False)
    print("Done.")

if __name__ == "__main__":
    main()
