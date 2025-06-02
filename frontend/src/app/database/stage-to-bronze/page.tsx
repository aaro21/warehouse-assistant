'use client';

import { useEffect, useState } from 'react';
import Breadcrumb from '@/components/Breadcrumb';

type StageToBronzeMapping = {
  stage_schema: string | null;
  stage_table_name: string | null;
  bronze_schema: string;
  bronze_table_name: string;
};

export default function StageToBronzePage() {
  const [data, setData] = useState<StageToBronzeMapping[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [persisting, setPersisting] = useState(false);
  const [persistMessage, setPersistMessage] = useState<string | null>(null);
  const [persistSuccess, setPersistSuccess] = useState<boolean | null>(null);

  const fetchMappings = async () => {
    setLoading(true);
    try {
      const response = await fetch('http://localhost:8000/lineage/extract/stage-to-bronze?persist=false');
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
    fetchMappings();
  }, []);

  const handlePersist = async () => {
    setPersisting(true);
    setPersistMessage(null);
    setPersistSuccess(null);
    try {
      const response = await fetch('http://localhost:8000/lineage/extract/stage-to-bronze?persist=true');
      if (!response.ok) {
        throw new Error(`HTTP error: ${response.status}`);
      }
      const json = await response.json();
      setPersistMessage('Persisted successfully!');
      setPersistSuccess(true);
      fetchMappings();
    } catch (err: any) {
      setPersistMessage(`Failed to persist: ${err.message}`);
      setPersistSuccess(false);
    } finally {
      setPersisting(false);
    }
  };

  return (
    <div className="p-6">
      <Breadcrumb
        items={[
          { label: 'Database', href: '/database' },
          { label: 'Stage to Bronze' },
        ]}
      />
      <h1 className="text-2xl font-bold mb-4">Stage to Bronze Mappings</h1>
      <p className="mb-4">
        This page displays mappings between your stage and bronze tables.
      </p>

      {/* Button and feedback */}
      <div className="mb-4">
        <button
          className={`px-4 py-2 rounded ${
            persisting
              ? 'bg-gray-400 cursor-not-allowed'
              : 'bg-blue-600 text-white hover:bg-blue-700'
          }`}
          onClick={handlePersist}
          disabled={persisting}
        >
          {persisting ? 'Persisting...' : 'Extract & Persist'}
        </button>
        {persistMessage && (
          <div className="mt-2">
            <span
              className={`${
                persistSuccess ? 'text-green-600' : 'text-red-600'
              }`}
            >
              {persistMessage}
            </span>
          </div>
        )}
      </div>

      {loading && <p>Loading...</p>}
      {error && <p className="text-red-500">Error: {error}</p>}

      {!loading && !error && data.length === 0 && (
        <p>No stage-to-bronze mappings found.</p>
      )}

      {!loading && data.length > 0 && (
        <table className="table-auto border-collapse border border-gray-300 w-full text-sm">
          <thead>
            <tr className="bg-gray-100">
              <th className="border border-gray-300 px-2 py-1">Stage Table</th>
              <th className="border border-gray-300 px-2 py-1">Bronze Table</th>
            </tr>
          </thead>
          <tbody>
            {data.map((item, i) => (
              <tr key={i}>
                <td className="border border-gray-300 px-2 py-1">
                  {item.stage_schema && item.stage_table_name
                    ? `${item.stage_schema}.${item.stage_table_name}`
                    : 'N/A'}
                </td>
                <td className="border border-gray-300 px-2 py-1">
                  {`${item.bronze_schema}.${item.bronze_table_name}`}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
