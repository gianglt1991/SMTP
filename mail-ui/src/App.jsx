import { useEffect, useState } from 'react';
import { Routes, Route, Link } from 'react-router-dom';
import axios from 'axios';

function UnsubscribeList() {
  const [emails, setEmails] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [search, setSearch] = useState('');
  const [newEmail, setNewEmail] = useState('');

  useEffect(() => {
    const fetchEmails = async () => {
      try {
        const response = await axios.get(`${process.env.REACT_APP_API_URL}/unsubscribe/list`, {
          headers: { Authorization: `Bearer ${localStorage.getItem('jwt')}` }
        });
        setEmails(response.data.emails);
      } catch (error) {
        setError(error.message);
      } finally {
        setLoading(false);
      }
    };
    fetchEmails();
  }, []);

  const handleAdd = async () => {
    try {
      await axios.post(`${process.env.REACT_APP_API_URL}/unsubscribe`, { email: newEmail }, {
        headers: { Authorization: `Bearer ${localStorage.getItem('jwt')}` }
      });
      setEmails([...emails, newEmail]);
      setNewEmail('');
    } catch (error) {
      setError(error.message);
    }
  };

  const handleDelete = async (email) => {
    try {
      await axios.delete(`${process.env.REACT_APP_API_URL}/unsubscribe/${email}`, {
        headers: { Authorization: `Bearer ${localStorage.getItem('jwt')}` }
      });
      setEmails(emails.filter((e) => e !== email));
    } catch (error) {
      setError(error.message);
    }
  };

  const filteredEmails = emails.filter((email) => email.includes(search.toLowerCase()));

  return (
    <div className="container mx-auto p-6">
      <h1 className="text-2xl font-bold mb-4">📭 Danh sách Unsubscribed</h1>
      {loading && <p>Loading...</p>}
      {error && <p className="text-red-500">{error}</p>}
      <div className="mb-4">
        <input
          type="text"
          placeholder="Search emails..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="input input-bordered w-full max-w-xs"
        />
      </div>
      <div className="mb-4">
        <input
          type="email"
          placeholder="Add new email"
          value={newEmail}
          onChange={(e) => setNewEmail(e.target.value)}
          className="input input-bordered mr-2"
        />
        <button
          onClick={handleAdd}
          className="btn btn-primary ml-2"
          disabled={!newEmail.match(/^\w+([-+.']\w+)*@[\w-]+\.\w+$/)}
        >
          Add
        </button>
      </div>
      <ul className="list-disc pl-5">
        {filteredEmails.map((email) => (
          <li key={email} className="flex items-center justify-between mb-2">
            <span>{email}</span>
            <button
              onClick={() => handleDelete(email)}
              className="btn btn-sm btn-error"
            >
              Remove
            </button>
          </li>
        ))}
      </ul>
    </div>
  );
}

function Dashboard() {
  return (
    <div className="container mx-auto p-6">
      <h1 className="text-2xl font-bold mb-4">📊 Dashboard</h1>
      <p>Coming soon: Metrics, charts, and system status.</p>
    </div>
  );
}

function Login() {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');

  const handleLogin = async (e) => {
    e.preventDefault();
    try {
      const response = await axios.post(`${process.env.REACT_APP_API_URL}/login`, {
        username,
        password,
      });
      localStorage.setItem('jwt', response.data.token);
      window.location.href = '/';
    } catch (error) {
      setError(error.response?.data?.message || 'Login failed');
    }
  };

  return (
    <div className="container mx-auto p-6">
      <h1 className="text-2xl font-bold mb-4">🔐 Login</h1>
      {error && <p className="text-red-500">{error}</p>}
      <div className="mb-4">
        <input
          type="text"
          placeholder="Username"
          value={username}
          onChange={(e) => setUsername(e.target.value)}
          className="input input-bordered w-full max-w-xs"
        />
      </div>
      <div className="mb-4">
        <input
          type="password"
          placeholder="Password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          className="input input-bordered w-full max-w-xs"
        />
      </div>
      <button onClick={handleLogin} className="btn btn-primary">
        Login
      </button>
    </div>
  );
}

function App() {
  return (
    <div>
      <nav className="bg-gray-800 p-4">
        <div className="container mx-auto flex justify-between">
          <Link to="/" className="text-white font-bold">Mail UI</Link>
          <div>
            <Link to="/" className="text-white mr-4">Unsubscribe</Link>
            <Link to="/dashboard" className="text-white">Dashboard</Link>
          </div>
        </div>
      </nav>
      <Routes>
        <Route path="/" element={<UnsubscribeList />} />
        <Route path="/dashboard" element={<Dashboard />} />
        <Route path="/login" element={<Login />} />
      </Routes>
    </div>
  );
}

export default App;
