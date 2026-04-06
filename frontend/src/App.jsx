import './index.css';
import LandingPage from './LandingPage';
import MerchantApp from './MerchantApp';
import BankApp from './BankApp';

function getSurface() {
  const path = window.location.pathname;
  if (path.startsWith('/merchant')) return 'merchant';
  if (path.startsWith('/bank'))     return 'bank';
  return 'landing';
}

export default function App() {
  const surface = getSurface();

  if (surface === 'merchant') return <MerchantApp />;
  if (surface === 'bank')     return <BankApp />;
  return <LandingPage />;
}

