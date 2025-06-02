'use client';

import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { oneDark, oneLight } from 'react-syntax-highlighter/dist/esm/styles/prism';
import { useEffect, useState } from 'react';
import { useParams } from 'next/navigation';
import Breadcrumb from '@/components/Breadcrumb';
import { Database } from 'lucide-react';

type ProcedureDetails = {
  proc_name: string;
  proc_hash: string;
  source_db: string;
  source_schema: string;
  proc_definition: string;
  record_insert_datetime: string;
};

type Mapping = {
  source_db: string;
  source_schema: string;
  source_table: string;
  source_column: string;
  target_db: string;
  target_schema: string;
  target_table: string;
  target_column: string;
  transform_expr?: string;
};

function MappingsGrid({ mappings }: { mappings: Mapping[] }) {
  if (!mappings.length) return <p>No mappings found.</p>;

  // Use first mapping as reference for table-level info
  const first = mappings[0];

  return (
    <div className="flex gap-8 w-full bg-white dark:bg-gray-900 rounded-2xl shadow-lg p-6 mb-6">
      {/* Source Card */}
      <div className="flex-1 bg-gray-50 dark:bg-gray-800 rounded-xl p-4 shadow">
        <div className="flex items-center mb-3">
          <Database className="text-blue-500 dark:text-blue-400 mr-2" size={22} />
          <span className="font-semibold text-lg">{first.source_db}</span>
        </div>
        <div className="text-gray-700 dark:text-gray-300 text-sm mb-2">
          {first.source_schema}
          <span className="mx-1 text-gray-400">·</span>
          {first.source_table}
        </div>
        <div>
          {mappings.map((m, i) => (
            <div
              key={i}
              className="flex items-center mb-1 px-2 py-1 rounded hover:bg-gray-100 dark:hover:bg-gray-700 relative"
            >
              <span className="font-mono">{m.source_column}</span>
              {/* Connector line/arrow */}
              <span
                className="absolute top-1/2 right-0 -translate-y-1/2 h-px w-6 bg-blue-500"
                style={{ marginLeft: '8px' }}
              />
              <svg
                className="absolute top-1/2 right-0 -translate-y-1/2 ml-6"
                width="12"
                height="12"
                viewBox="0 0 24 24"
                fill="none"
                stroke="#3b82f6"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
              >
                <line x1="0" y1="12" x2="18" y2="12" />
                <polyline points="12 6 18 12 12 18" />
              </svg>
            </div>
          ))}
        </div>
      </div>
      {/* Destination Card */}
      <div className="flex-1 bg-gray-50 dark:bg-gray-800 rounded-xl p-4 shadow">
        <div className="flex items-center mb-3">
          <Database className="text-green-500 dark:text-green-400 mr-2" size={22} />
          <span className="font-semibold text-lg">{first.target_db || 'wh_silver'}</span>
        </div>
        <div className="text-gray-700 dark:text-gray-300 text-sm mb-2">
          {first.target_schema}
          <span className="mx-1 text-gray-400">·</span>
          {first.target_table}
        </div>
        <div>
          {mappings.map((m, i) => (
            <div
              key={i}
              className="flex items-center mb-1 px-2 py-1 rounded hover:bg-gray-100 dark:hover:bg-gray-700"
            >
              <span className="font-mono">{m.target_column}</span>
              {m.transform_expr && (
                <span className="ml-2 text-xs text-gray-400 italic">{m.transform_expr}</span>
              )}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

export default function ProcedureDetailPage() {
  const params = useParams();
  const procHash = params?.proc_hash as string;
  const [proc, setProc] = useState<ProcedureDetails | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // For AI extraction
  const [mappings, setMappings] = useState<Mapping[] | null>(null);
  const [aiLoading, setAiLoading] = useState(false);
  const [aiError, setAiError] = useState<string | null>(null);

  // For saving mappings
  const [saveStatus, setSaveStatus] = useState<'idle' | 'saving' | 'success' | 'error'>('idle');
  const [saveError, setSaveError] = useState<string | null>(null);

  // Dark mode detection
  const [isDarkMode, setIsDarkMode] = useState(false);

  useEffect(() => {
    if (typeof window !== 'undefined') {
      const match = window.matchMedia('(prefers-color-scheme: dark)');
      setIsDarkMode(match.matches);
      const handler = (e: MediaQueryListEvent) => setIsDarkMode(e.matches);
      match.addEventListener('change', handler);
      return () => match.removeEventListener('change', handler);
    }
  }, []);

  useEffect(() => {
    if (!procHash) return;
    setLoading(true);
    fetch(`http://localhost:8000/lineage/procedures/${procHash}`)
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      })
      .then((data) => setProc(data))
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, [procHash]);

  // Handler for AI analyze button
  const handleAnalyzeClick = async () => {
    setAiLoading(true);
    setAiError(null);
    setMappings(null);

    try {
      const res = await fetch(`http://localhost:8000/lineage/procedures/${procHash}/analyze`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      setMappings(data.mappings || []);
    } catch (err: any) {
      setAiError(err.message);
    } finally {
      setAiLoading(false);
    }
  };

  return (
    <div className="p-6">
      <Breadcrumb
        items={[
          { label: 'Database', href: '/database' },
          { label: 'Bronze → Silver → Gold', href: '/database/silver-gold-procs' },
          { label: proc ? proc.proc_name : procHash },
        ]}
      />
      <h1 className="text-2xl font-bold mb-4">Procedure Details</h1>
      {loading && <p>Loading...</p>}
      {error && <p className="text-red-500">Error: {error}</p>}

      {!loading && !error && proc && (
        <div>
          <button
            className="mb-4 px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700"
            onClick={handleAnalyzeClick}
            disabled={aiLoading}
          >
            {aiLoading ? "Analyzing..." : "Analyze Procedure with AI"}
          </button>
          {aiError && <div className="text-red-500 mb-2">Error: {aiError}</div>}

          <div className="mb-2">
            <span className="font-semibold">Name:</span> {proc.proc_name}
          </div>
          <div className="mb-2">
            <span className="font-semibold">Hash:</span>{' '}
            <span className="font-mono">{proc.proc_hash}</span>
          </div>
          <div className="mb-2">
            <span className="font-semibold">Database:</span> {proc.source_db}
          </div>
          <div className="mb-2">
            <span className="font-semibold">Schema:</span> {proc.source_schema}
          </div>
          <div className="mb-2">
            <span className="font-semibold">Inserted:</span>{' '}
            {new Date(proc.record_insert_datetime).toLocaleString()}
          </div>

          {/* Modern Two-Card Mappings Section */}
          {mappings && (
            <div>
              <h2 className="font-semibold mb-2">Extracted Mappings</h2>
              <MappingsGrid mappings={mappings} />
              <div className="mt-4 flex flex-col items-start">
                <button
                  className={`mb-6 px-4 py-2 bg-green-600 text-white rounded hover:bg-green-700 disabled:opacity-50`}
                  onClick={async () => {
                    if (!mappings || mappings.length === 0) return;
                    setSaveStatus('saving');
                    setSaveError(null);
                    try {
                      const res = await fetch(
                        `http://localhost:8000/lineage/procedures/${procHash}/mappings`,
                        {
                          method: 'POST',
                          headers: { 'Content-Type': 'application/json' },
                          body: JSON.stringify(mappings),
                        }
                      );
                      if (!res.ok) throw new Error(`HTTP ${res.status}`);
                      setSaveStatus('success');
                    } catch (err: any) {
                      setSaveError(err.message || 'Failed to save');
                      setSaveStatus('error');
                    }
                  }}
                  disabled={saveStatus === 'saving'}
                >
                  {saveStatus === 'saving' ? 'Saving...' : 'Approve & Save Mappings'}
                </button>
                {saveStatus === 'success' && (
                  <span className="mt-2 text-green-600">Saved!</span>
                )}
                {saveStatus === 'error' && (
                  <span className="mt-2 text-red-500">Error: {saveError}</span>
                )}
              </div>
            </div>
          )}

          <div className="mb-4 bg-white dark:bg-gray-900 rounded-2xl shadow-lg p-6">
            <span className="font-semibold">Definition:</span>
            <div className="mt-2 rounded border border-gray-300 dark:border-gray-700 overflow-auto text-xs">
              <SyntaxHighlighter
                language="sql"
                style={isDarkMode ? oneDark : oneLight}
                customStyle={{
                  margin: 0,
                  background: "inherit",
                  borderRadius: "0.375rem",
                  fontSize: "0.875em"
                }}
                showLineNumbers
              >
                {proc.proc_definition}
              </SyntaxHighlighter>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}