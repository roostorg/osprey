export interface FeatureInfo {
  name: string;
  source_file: string;
  source_line: number;
  category: string;
  type_annotation: string | null;
  extraction_fn: string;
  definition: string;
  referenced_by_rules: string[];
  referenced_by_features: string[];
  referenced_by_whenrules: number;
  total_references: number;
}

export interface FeaturesListResponse {
  features: FeatureInfo[];
  total: number;
  categories: Record<string, number>;
  extraction_fns: Record<string, number>;
}
