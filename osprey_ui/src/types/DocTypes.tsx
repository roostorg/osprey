export interface UdfCategory {
  name: string | null;
  udfs: UdfMethodSpec[];
}

export interface UdfMethodSpec {
  name: string;
  doc: string | null;
  argument_specs: UdfArgumentSpec[];
  return_type: string;
  category: string | null;
}

export interface UdfArgumentSpec {
  name: string;
  type: string;
  default: string | null;
  doc: string | null;
}
