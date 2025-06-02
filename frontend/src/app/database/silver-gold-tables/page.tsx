'use client';

import { useEffect, useState } from 'react';
import Breadcrumb from '@/components/Breadcrumb';

type SilverGoldTableItem = {
  src_db: string | null;
  src_schema: string | null;
  src_table: string | null;
  role: string | null;
  record_insert_datetime: string | null;
};

export default function SilverGoldTablesPage() {
  const [data, setData] = useState<SilverGoldTableItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [persisting, setPersisting] = useState(false);
  const [message, setMessage] = useState<string | null>(null);

  const fetchSilverGoldTables = async () => {
    setLoading(true);
    try {
      const response = await fetch('http://localhost:8000/lineage/view/silver-gold-tables');
      if (!response.ok) {
        throw new Error(`HTTP error: ${response.status}`);
      }
      const json = await response.json();
      setData(json);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchSilverGoldTables();
  }, []);

  const handleExtractPersist = async () => {
    setPersisting(true);
    setMessage(null);
    try {
      const response = await fetch('http://localhost:8000/lineage/load/silver-gold-tables', {
        method: 'POST',
      });
      if (!response.ok) {
        throw new Error(`HTTP error: ${response.status}`);
      }
      setMessage('Persisted successfully!');
      await fetchSilverGoldTables();
    } catch (err: any) {
      setMessage(`Error: ${err.message}`);
    } finally {
      setPersisting(false);
    }
  };

  return (
    <div className="p-6">
      <Breadcrumb
        items={[
          { label: 'Database', href: '/database' },
          { label: 'Silver & Gold Tables' },
        ]}
      />
      <h1 className="text-2xl font-bold mb-4">Silver & Gold Tables</h1>
      <p className="mb-4">This page displays silver and gold tables in your warehouse.</p>

      <button
        onClick={handleExtractPersist}
        disabled={persisting}
        className="mb-4 px-4 py-2 rounded bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50"
      >
        {persisting ? 'Persisting...' : 'Extract & Persist'}
      </button>
      {message && (
        <div className={`mb-4 ${message.startsWith('Error') ? 'text-red-500' : 'text-green-600'}`}>
          {message}
        </div>
      )}

      {loading && <p>Loading...</p>}
      {error && <p className="text-red-500">Error: {error}</p>}

      {!loading && !error && data.length === 0 && (
        <p>No silver & gold tables found.</p>
      )}

      {!loading && data.length > 0 && (
        <table className="table-auto border-collapse border border-gray-300 w-full text-sm">
          <thead>
            <tr className="bg-gray-100">
              <th className="border border-gray-300 px-2 py-1">DB</th>
              <th className="border border-gray-300 px-2 py-1">Schema</th>
              <th className="border border-gray-300 px-2 py-1">Table</th>
              <th className="border border-gray-300 px-2 py-1">Role</th>
              <th className="border border-gray-300 px-2 py-1">Inserted At</th>
            </tr>
          </thead>
          <tbody>
            {data.map((item, idx) => (
              <tr key={idx}>
                <td className="border border-gray-300 px-2 py-1">{item.src_db ?? '-'}</td>
                <td className="border border-gray-300 px-2 py-1">{item.src_schema ?? '-'}</td>
                <td className="border border-gray-300 px-2 py-1">{item.src_table ?? '-'}</td>
                <td className="border border-gray-300 px-2 py-1">{item.role ?? '-'}</td>
                <td className="border border-gray-300 px-2 py-1">{item.record_insert_datetime ?? '-'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}