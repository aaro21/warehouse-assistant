'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';
import Breadcrumb from '@/components/Breadcrumb';

type ProcItem = {
  proc_name: string;
  proc_hash: string;
  source_db: string;
  source_schema: string;
  record_insert_datetime: string;
};

export default function SilverGoldProcsPage() {
  const [data, setData] = useState<ProcItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [persisting, setPersisting] = useState(false);
  const [success, setSuccess] = useState<string | null>(null);
  const [persistError, setPersistError] = useState<string | null>(null);

  const [analyzing, setAnalyzing] = useState(false);
  const [analyzeStatus, setAnalyzeStatus] = useState<string | null>(null);
  const [analyzeError, setAnalyzeError] = useState<string | null>(null);

  useEffect(() => {
    const fetchProcs = async () => {
      try {
        const response = await fetch('http://localhost:8000/lineage/discover/silver-gold-procs');
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

    fetchProcs();
  }, []);

  const handlePersist = async () => {
    setPersisting(true);
    setSuccess(null);
    setPersistError(null);
    try {
      const response = await fetch('http://localhost:8000/lineage/discover/silver-gold-procs?persist=true');
      if (!response.ok) {
        throw new Error(`HTTP error: ${response.status}`);
      }
      setSuccess('Extraction and persistence completed successfully.');
    } catch (err: any) {
      setPersistError(err.message);
    } finally {
      setPersisting(false);
    }
  };

  const handleAnalyzeAll = async () => {
    setAnalyzing(true);
    setAnalyzeStatus(null);
    setAnalyzeError(null);
    try {
      for (const item of data) {
        const resp = await fetch(
          `http://localhost:8000/lineage/procedures/${item.proc_hash}/mappings`,
          {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify([]) // empty mapping list
          }
        );
        if (!resp.ok) {
          throw new Error(`HTTP error while analyzing ${item.proc_name}: ${resp.status}`);
        }
      }
      setAnalyzeStatus('All procedures analyzed successfully.');
    } catch (err: any) {
      setAnalyzeError(err.message);
    } finally {
      setAnalyzing(false);
    }
  };

  return (
    <div className="p-6">
      <Breadcrumb
        items={[
          { label: 'Database', href: '/database' },
          { label: 'Silver-Gold Procs' }
        ]}
      />
      <h1 className="text-2xl font-bold mb-4">Silver-Gold Procedures</h1>
      <p className="mb-4">
        This page lists stored procedures extracted from the silver and gold layers.
      </p>

      {!loading && (
        <>
          <button
            onClick={handlePersist}
            disabled={loading || persisting || analyzing}
            className="mb-2 px-4 py-2 bg-blue-600 text-white rounded disabled:opacity-50"
          >
            Extract & Persist
          </button>
          <button
            onClick={handleAnalyzeAll}
            disabled={loading || persisting || analyzing}
            className="mb-2 ml-2 px-4 py-2 bg-green-600 text-white rounded disabled:opacity-50"
          >
            Analyze All
          </button>
          {success && <p className="text-green-600 mb-2">{success}</p>}
          {persistError && <p className="text-red-600 mb-2">Error: {persistError}</p>}
          {analyzeStatus && <p className="text-green-600 mb-2">{analyzeStatus}</p>}
          {analyzeError && <p className="text-red-600 mb-2">Error: {analyzeError}</p>}
        </>
      )}

      {loading && <p>Loading...</p>}
      {error && <p className="text-red-500">Error: {error}</p>}

      {!loading && !error && data.length === 0 && (
        <p>No stored procedures found.</p>
      )}

      {!loading && data.length > 0 && (
        <table className="table-auto border-collapse border border-gray-300 w-full text-sm">
          <thead>
            <tr className="bg-gray-100">
              <th className="border border-gray-300 px-2 py-1">Procedure Name</th>
              <th className="border border-gray-300 px-2 py-1">Hash</th>
              <th className="border border-gray-300 px-2 py-1">Source DB</th>
              <th className="border border-gray-300 px-2 py-1">Schema</th>
              <th className="border border-gray-300 px-2 py-1">Inserted</th>
            </tr>
          </thead>
          <tbody>
            {data.map((item, idx) => (
              <tr key={item.proc_hash + idx}>
                <td className="border border-gray-300 px-2 py-1">
                  <Link
                    href={`/database/procedures/${item.proc_hash}`}
                    className="text-blue-600 underline hover:text-blue-800"
                  >
                    {item.proc_name}
                  </Link>
                </td>
                <td className="border border-gray-300 px-2 py-1 font-mono break-all">{item.proc_hash.slice(0, 10) + '...'}</td>
                <td className="border border-gray-300 px-2 py-1">{item.source_db}</td>
                <td className="border border-gray-300 px-2 py-1">{item.source_schema}</td>
                <td className="border border-gray-300 px-2 py-1">{item.record_insert_datetime}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}