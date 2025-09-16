import { DagreLayoutOptions } from 'cytoscape-dagre';
import { Core, CssStyleDeclaration, ElementsDefinition, NodeSingular } from 'cytoscape';
import { ReactElement } from 'react';

export interface HierarchicalGraphOptions {
  elements: ElementsDefinition;
  nodeStyle?: Partial<CssStyleDeclaration>;
  edgeStyle?: Partial<CssStyleDeclaration>;
  layoutOptions?: Partial<DagreLayoutOptions>;
  onLoad?: (cy: Core) => void;
  ToolTip?: ({ node }: { node: NodeSingular }) => ReactElement;
}

export interface Node {
  id: number;
  name: string;
  num_children: number;
  type: string;
  label_type?: string;
  label_name?: string;
  entity_name?: string;
  file_path: string;
  value: string | string[];
}

export interface Edge {
  source: number;
  target: number;
  weight: number;
  color: string;
}

export enum NodeType {
  Label = 'Label',
  Rule = 'Rule',
}

export enum LabelType {
  Check = 'HasLabel',
  Add = 'LabelAdd',
  Remove = 'LabelRemove',
}
