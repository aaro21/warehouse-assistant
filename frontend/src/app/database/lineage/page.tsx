'use client';

import { useEffect, useState } from 'react';
import Breadcrumb from '@/components/Breadcrumb';

type LineageItem = {
  lineage_id: number;
  stage_db: string;
  stage_schema: string;
  stage_table: string;
  bronze_db: string;
  bronze_schema: string;
  bronze_table: string;
  silver_db: string;
  silver_schema: string;
  silver_table: string;
  gold_db: string;
  gold_schema: string;
  gold_table: string;
};

export default function LineagePage() {
  const [data, setData] = useState<LineageItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchLineage = async () => {
      try {
        const response = await fetch('http://localhost:8000/lineage/flat');
        if (!response.ok) {
          throw new Error(`HTTP error: ${response.status}`);
        }
        const json = await response.json();
        console.log('Lineage data:', json); // ✅ confirm this logs
        setData(json);
      } catch (err: any) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };

    fetchLineage();
  }, []);

  return (
    <div className="p-6">
      <Breadcrumb
        items={[
          { label: 'Database', href: '/database' },
          { label: 'Lineage' },
        ]}
      />
      <h1 className="text-2xl font-bold mb-4">Lineage</h1>
      <p className="mb-4">This page will visualize lineage data between your databases.</p>

      {loading && <p>Loading...</p>}
      {error && <p className="text-red-500">Error: {error}</p>}

      {!loading && !error && data.length === 0 && (
        <p>No lineage data available.</p>
      )}

      {!loading && data.length > 0 && (
        <table className="table-auto border-collapse border border-gray-300 w-full text-sm">
          <thead>
            <tr className="bg-gray-100">
              <th className="border border-gray-300 px-2 py-1">Stage</th>
              <th className="border border-gray-300 px-2 py-1">Bronze</th>
              <th className="border border-gray-300 px-2 py-1">Silver</th>
              <th className="border border-gray-300 px-2 py-1">Gold</th>
            </tr>
          </thead>
          <tbody>
            {data.map((item) => (
              <tr key={item.lineage_id}>
                <td className="border border-gray-300 px-2 py-1">{`${item.stage_db}.${item.stage_schema}.${item.stage_table}`}</td>
                <td className="border border-gray-300 px-2 py-1">{`${item.bronze_db}.${item.bronze_schema}.${item.bronze_table}`}</td>
                <td className="border border-gray-300 px-2 py-1">{`${item.silver_db}.${item.silver_schema}.${item.silver_table}`}</td>
                <td className="border border-gray-300 px-2 py-1">{`${item.gold_db}.${item.gold_schema}.${item.gold_table}`}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}