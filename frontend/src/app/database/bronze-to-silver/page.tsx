'use client';

import { useEffect, useState } from 'react';
import Breadcrumb from '@/components/Breadcrumb';
import Button from '@/components/Button';
import axios from 'axios';

export default function BronzeToSilverPage() {
  const [mappings, setMappings] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [persisting, setPersisting] = useState(false);
  const [status, setStatus] = useState<string | null>(null);

  useEffect(() => {
    const fetchMappings = async () => {
      try {
        const response = await axios.get('/lineage/extract/bronze-to-silver');
        setMappings(response.data);
      } catch (err) {
        setStatus('Failed to load mappings.');
      } finally {
        setLoading(false);
      }
    };
    fetchMappings();
  }, []);

  const persist = async () => {
    setPersisting(true);
    setStatus(null);
    try {
      await axios.get('/lineage/extract/bronze-to-silver?persist=true');
      setStatus('Mappings persisted!');
    } catch {
      setStatus('Failed to persist.');
    }
    setPersisting(false);
  };

  return (
    <div className="p-4 space-y-4">
      <Breadcrumb path={["Database", "Bronze ➝ Silver Lineage"]} />
      <h1 className="text-2xl font-semibold">Bronze ➝ Silver Table Lineage</h1>
      {loading ? <p>Loading...</p> : (
        <table className="w-full table-auto border">
          <thead>
            <tr>
              <th className="border px-2 py-1">Bronze Table</th>
              <th className="border px-2 py-1">Silver Table</th>
            </tr>
          </thead>
          <tbody>
            {mappings.map((m, i) => (
              <tr key={i}>
                <td className="border px-2 py-1">{`${m.bronze_db}.${m.bronze_schema}.${m.bronze_table}`}</td>
                <td className="border px-2 py-1">{`${m.silver_db}.${m.silver_schema}.${m.silver_table}`}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      <div className="flex items-center gap-4">
        <Button onClick={persist} disabled={persisting}>
          {persisting ? 'Persisting...' : 'Persist Mappings'}
        </Button>
        {status && <p className="text-sm text-gray-600">{status}</p>}
      </div>
    </div>
  );
}