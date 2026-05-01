import './utils/ErrorReporting';
import './utils/DayjsSetup';
import ReactDOM from 'react-dom/client';

import 'antd/dist/reset.css';
import './index.css';
import App from './App';

const root = ReactDOM.createRoot(document.getElementById('root')!);
root.render(<App />);
