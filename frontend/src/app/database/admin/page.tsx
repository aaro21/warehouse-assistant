'use client';
import { useState } from 'react';
import Button from '@/components/Button';
import Breadcrumb from '@/components/Breadcrumb';

export default function DatabaseAdminPage() {
  const [loading, setLoading] = useState(false);
  const [success, setSuccess] = useState<boolean | null>(null);

  const triggerStageToBronzeLoad = async () => {
    setLoading(true);
    setSuccess(null);
    try {
      const response = await fetch('http://localhost:8000/lineage/extract/stage-to-bronze?persist=true');
      if (!response.ok) {
        throw new Error(`Failed with status ${response.status}`);
      }
      setSuccess(true);
    } catch (err) {
      console.error('Error triggering stage-to-bronze load:', err);
      setSuccess(false);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="p-6">
      <Breadcrumb
        items={[
          { label: 'Database', href: '/database' },
          { label: 'Admin' },
        ]}
      />
      <h1 className="text-2xl font-bold mb-4">Database Admin Tools</h1>

      <div className="space-y-4">
        <Button onClick={triggerStageToBronzeLoad} disabled={loading}>
          {loading ? 'Loading...' : 'Extract & Persist Stage → Bronze'}
        </Button>

        {success === true && (
          <p className="text-green-600">Stage-to-bronze mappings successfully persisted.</p>
        )}
        {success === false && (
          <p className="text-red-600">Failed to persist stage-to-bronze mappings. Check backend logs.</p>
        )}
      </div>
    </div>
  );
} 