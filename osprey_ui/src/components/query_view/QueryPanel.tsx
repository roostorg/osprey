
import { Route, Switch } from 'react-router-dom';

import EntityPanel from '../entities/EntityPanel';
import QueryInput, { QueryInputProps } from './QueryInput';
import QueryListPanel from './QueryListPanel';

import { Routes } from '../../Constants';
import 'highlight.js/styles/atelier-cave-light.css';

const QueryPanel = ({ ...props }: QueryInputProps) => {
  return (
    <>
      <QueryInput {...props} />
      <Switch>
        <Route path={Routes.ENTITY}>
          <EntityPanel />
        </Route>
        <Route>
          <QueryListPanel />
        </Route>
      </Switch>
    </>
  );
};

export default QueryPanel;
