

'use client';

import Link from 'next/link';

export default function DatabaseDashboardPage() {
  return (
    <div className="p-6 bg-gray-50 min-h-screen">
      <div className="max-w-5xl mx-auto">
        <h1 className="text-4xl font-extrabold mb-6 text-gray-800">Data Warehouse Dashboard</h1>
        <p className="mb-8 text-lg text-gray-600">
          Explore insights and trace the flow of data through various warehouse stages: <strong>Stage → Bronze → Silver → Gold</strong>.
        </p>

        <div className="grid gap-6 grid-cols-1 md:grid-cols-2 lg:grid-cols-3">
          <div className="bg-white border border-gray-200 rounded-xl p-6 shadow-sm hover:shadow-md hover:scale-[1.02] transition transform">
            <h2 className="text-xl font-semibold mb-2">📊 Lineage Viewer</h2>
            <p className="mb-3 text-sm text-gray-600">
              Explore how tables move and transform through the warehouse layers.
            </p>
            <Link href="/database/lineage" className="inline-block text-blue-600 hover:underline font-medium">
              View Lineage →
            </Link>
          </div>

          <div className="bg-white border border-gray-200 rounded-xl p-6 shadow-sm hover:shadow-md hover:scale-[1.02] transition transform">
            <h2 className="text-xl font-semibold mb-2">🧬 Schema Mapping</h2>
            <p className="mb-3 text-sm text-gray-600">
              Coming soon: visualize schema-level mappings and structure changes.
            </p>
            <span className="text-gray-400 italic">In development</span>
          </div>

          <div className="bg-white border border-gray-200 rounded-xl p-6 shadow-sm hover:shadow-md hover:scale-[1.02] transition transform">
            <h2 className="text-xl font-semibold mb-2">⏱️ Data Freshness</h2>
            <p className="mb-3 text-sm text-gray-600">
              Coming soon: monitor data sync times and update frequency.
            </p>
            <span className="text-gray-400 italic">In development</span>
          </div>
        </div>
      </div>
    </div>
  );
}