'use client';

import { useEffect, useState } from 'react';
import Breadcrumb from '@/components/Breadcrumb';
import Button from '@/components/Button';
import axios from 'axios';

export default function SilverGoldLineagePage() {
  const [procs, setProcs] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [persisting, setPersisting] = useState(false);
  const [status, setStatus] = useState<string | null>(null);

  const loadPreview = async () => {
  setLoading(true);
  try {
    // Directly call your FastAPI backend!
    const response = await axios.get('http://localhost:8000/lineage/extract/silver-to-gold/preview');
    setProcs(response.data);
  } catch (err) {
    console.error('Failed to load procedures', err);
  }
  setLoading(false);
};

  const persistMappings = async () => {
    setPersisting(true);
    try {
      const response = await axios.post('/lineage/extract/silver-to-gold', procs);
      setStatus(response.data.message || 'Successfully persisted mappings.');
    } catch (err) {
      console.error('Persistence failed', err);
      setStatus('Error persisting mappings');
    }
    setPersisting(false);
  };

  useEffect(() => {
    loadPreview();
  }, []);

  return (
    <div className="p-4 space-y-4">
      <Breadcrumb path={["Database", "Silver ➝ Gold Lineage"]} />
      <h1 className="text-2xl font-semibold">Silver ➝ Gold Procedure Lineage</h1>

      {loading ? (
        <p>Loading procedures...</p>
      ) : (
        <table className="w-full table-auto border">
          <thead>
            <tr>
              <th className="border px-2 py-1">Procedure</th>
              <th className="border px-2 py-1">Source DB</th>
              <th className="border px-2 py-1">Schema</th>
              <th className="border px-2 py-1">Table</th>
            </tr>
          </thead>
          <tbody>
            {procs.map((p) => (
              <tr key={p.proc_id}>
                <td className="border px-2 py-1">{p.proc_name}</td>
                <td className="border px-2 py-1">{p.source_db}</td>
                <td className="border px-2 py-1">{p.source_schema}</td>
                <td className="border px-2 py-1">{p.source_table}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      <div className="flex items-center gap-4">
        <Button onClick={persistMappings} disabled={persisting}>
          {persisting ? 'Persisting...' : 'Persist Mappings'}
        </Button>
        {status && <p className="text-sm text-gray-600">{status}</p>}
      </div>
    </div>
  );
}