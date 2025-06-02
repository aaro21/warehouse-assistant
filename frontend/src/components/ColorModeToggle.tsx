'use client';
import { useColorMode } from '@/context/ColorModeContext';
import { Sun, Moon } from 'lucide-react';

export default function ColorModeToggle() {
  const { colorMode, toggleColorMode } = useColorMode();

  return (
    <button
      onClick={toggleColorMode}
      className="flex items-center px-3 py-2 rounded transition hover:bg-gray-200 dark:hover:bg-gray-700 focus:outline-none"
      aria-label={`Switch to ${colorMode === 'light' ? 'dark' : 'light'} mode`}
      type="button"
    >
      {colorMode === 'light' ? <Moon size={18} className="mr-2" /> : <Sun size={18} className="mr-2" />}
      <span className="text-xs font-semibold">{colorMode === 'light' ? 'Dark Mode' : 'Light Mode'}</span>
    </button>
  );
}