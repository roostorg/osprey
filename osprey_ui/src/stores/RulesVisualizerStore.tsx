import create from 'zustand';
import { Node, Edge } from '../types/RulesVisualizerTypes';

interface RuleVizGraph {
  nodes: Node[] | null;
  edges: Edge[] | null;
  selectedFeature?: string;
  selectedFeatureType?: 'Action' | 'Label';
  errorMessage?: string;
}

type RulesVisualizerStore = {
  updateRuleVizGraph: (ruleVizGraph: RuleVizGraph) => void;
} & RuleVizGraph;

const useRulesVisualizerStore = create<RulesVisualizerStore>((set) => ({
  nodes: null,
  edges: null,
  updateRuleVizGraph: (ruleVizGraph: RuleVizGraph) => set(() => ({ ...ruleVizGraph })),
}));

export default useRulesVisualizerStore;
