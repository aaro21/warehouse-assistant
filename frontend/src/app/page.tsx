"use client";

import { useState } from 'react';

export default function HomePage() {
  const [query, setQuery] = useState('');
  const [response, setResponse] = useState('');
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setResponse('');
    try {
      const res = await fetch('http://localhost:8000/lineage/query/ai-sql', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question: query }),
      });
      const data = await res.json();
      setResponse(data?.answer || 'No answer returned.');
    } catch (error) {
      setResponse('Error fetching response.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="max-w-3xl mx-auto p-4">
      <h1 className="text-2xl font-bold mb-4">AI SQL Agent</h1>
      <form onSubmit={handleSubmit} className="mb-4">
        <textarea
          className="w-full p-2 border rounded mb-2"
          rows={4}
          placeholder="Ask a question about the data warehouse..."
          value={query}
          onChange={(e) => setQuery(e.target.value)}
        />
        <button
          type="submit"
          className="px-4 py-2 bg-blue-600 text-white rounded disabled:opacity-50"
          disabled={loading || !query.trim()}
        >
          {loading ? 'Thinking...' : 'Ask'}
        </button>
      </form>
      {response && (
        <pre className="whitespace-pre-wrap bg-gray-100 p-4 rounded border">
          {response}
        </pre>
      )}
    </div>
  );
}
