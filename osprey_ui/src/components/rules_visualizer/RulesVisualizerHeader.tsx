import { useState } from 'react';
import { AutoComplete, Button, Drawer, Input, Space } from 'antd';
import { OptionData, OptionGroupData } from 'rc-select/lib/interface/index';
import { Core } from 'cytoscape';
import shallow from 'zustand/shallow';

import useApplicationConfigStore from '../../stores/ApplicationConfigStore';
import useRulesVisualizerStore from '../../stores/RulesVisualizerStore';
import { getGraphJson } from '../../actions/RulesVisualizerActions';
import Text, { TextSizes } from '../../uikit/Text';
import { DEFAULT_ANIMATE_DURATION } from './RulesVisualizer';
import { LabelInfo } from '../../stores/ApplicationConfigStore';

import styles from './RulesVisualizerHeader.module.css';

const RulesVisualizerHeader = ({
  cy,
  setIsLoading,
  showLabelUpstream,
  showLabelDownstream,
}: {
  cy: Core | null;
  setIsLoading: Function;
  showLabelUpstream: boolean;
  showLabelDownstream: boolean;
}) => {
  const [isDrawerVisible, setIsDrawerVisible] = useState(false);
  const [labelInfoMapping, knownActionNames] = useApplicationConfigStore(
    (state) => [state.labelInfoMapping, state.knownActionNames],
    shallow
  );
  const [updateRuleVizGraph, nodes, selectedFeature, selectedFeatureType] = useRulesVisualizerStore(
    (state) => [state.updateRuleVizGraph, state.nodes, state.selectedFeature, state.selectedFeatureType],
    shallow
  );

  const selectedFeatureText = (
    <Space>
      <Text size={TextSizes.H5}>{selectedFeatureType}</Text>
      <Text size={TextSizes.H7}>{selectedFeature}</Text>
    </Space>
  );

  const handleFeatureSelect = async (value: string, e: OptionData | OptionGroupData) => {
    setIsLoading(true);
    updateRuleVizGraph({ nodes: null, edges: null });
    const graphJson = await getGraphJson(e.type, [`${e.value}`], showLabelUpstream, showLabelDownstream);
    updateRuleVizGraph(graphJson);
    setIsLoading(false);
  };

  const handleNodeSelect = (value: string, e: OptionData | OptionGroupData) => {
    if (cy) {
      cy.animate({
        easing: 'ease-out-cubic',
        duration: DEFAULT_ANIMATE_DURATION,
        fit: { eles: cy.elements(), padding: 0 },
        complete: () => {
          cy.animate({
            easing: 'ease-in-cubic',
            duration: DEFAULT_ANIMATE_DURATION,
            fit: { eles: cy.getElementById(e.id), padding: 300 },
          });
        },
      });
    }
    setIsDrawerVisible(false);
  };

  const drawer = (
    <Drawer
      title={selectedFeatureText}
      placement="right"
      closable={false}
      onClose={() => setIsDrawerVisible(false)}
      visible={isDrawerVisible}
      width="600"
    >
      <div className={styles.drawerBody}>
        <AutoComplete
          className={styles.autocomplete}
          options={nodes?.map((node) => ({ value: node.name, id: node.id }))}
          onSelect={handleNodeSelect}
          filterOption
        >
          <Input.Search size="large" placeholder="Search by node" enterButton allowClear />
        </AutoComplete>
      </div>
    </Drawer>
  );

  return (
    <div className={styles.header}>
      <Text className={styles.leftHeaderContainer} size={TextSizes.H3}>
        Rules Visualizer
      </Text>
      <div className={styles.rightHeaderContainer}>
        <Space>
          {selectedFeature && (
            <Button size="large" onClick={() => setIsDrawerVisible(true)}>
              {selectedFeatureText}
            </Button>
          )}
          <AutoComplete
            className={styles.autocomplete}
            options={getSuggestions(knownActionNames, labelInfoMapping)}
            onSelect={handleFeatureSelect}
            filterOption
            defaultOpen
            autoFocus
          >
            <Input.Search size="large" placeholder="Search by action or label" enterButton allowClear />
          </AutoComplete>
        </Space>
      </div>
      {drawer}
    </div>
  );
};

function getSuggestions(actions: Set<String>, labels: Map<string, LabelInfo>) {
  return [
    {
      label: 'Actions',
      options: [...actions].map((action) => ({
        value: action,
        type: 'action',
      })),
    },
    {
      label: 'Labels',
      options: [...labels.keys()].map((label) => ({
        value: label,
        type: 'label',
      })) as OptionData[],
    },
  ] as OptionGroupData[];
}

export default RulesVisualizerHeader;
