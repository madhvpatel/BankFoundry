import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import './revenueRecovery.css';
import RevenueRecoveryApp from './RevenueRecoveryApp.jsx';

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <RevenueRecoveryApp />
  </StrictMode>,
);
