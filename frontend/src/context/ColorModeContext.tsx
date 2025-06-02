'use client';
import { createContext, useContext, useEffect, useState, ReactNode } from 'react';

// Fix hydration warning: only show children after mount

type ColorMode = 'light' | 'dark';
type Context = { colorMode: ColorMode; toggleColorMode: () => void; };

const ColorModeContext = createContext<Context | undefined>(undefined);

export function ColorModeProvider({ children }: { children: ReactNode }) {
  const [colorMode, setColorMode] = useState<ColorMode>(() => {
    // Try to initialize from localStorage or system preference
    if (typeof window !== 'undefined') {
      const stored = localStorage.getItem('color-mode');
      if (stored === 'dark' || stored === 'light') return stored;
      return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
    }
    return 'light';
  });

  const [mounted, setMounted] = useState(false);
  useEffect(() => {
    setMounted(true);
  }, []);

  // Always keep html class and localStorage in sync with state
  useEffect(() => {
    if (typeof window === 'undefined') return;
    if (colorMode === 'dark') {
      document.documentElement.classList.add('dark');
    } else {
      document.documentElement.classList.remove('dark');
    }
    localStorage.setItem('color-mode', colorMode);
  }, [colorMode]);

  const toggleColorMode = () => {
    setColorMode((prev) => (prev === 'light' ? 'dark' : 'light'));
  };

  if (!mounted) return null;
  return (
    <ColorModeContext.Provider value={{ colorMode, toggleColorMode }}>
      {children}
    </ColorModeContext.Provider>
  );
}

export function useColorMode() {
  const ctx = useContext(ColorModeContext);
  if (!ctx) throw new Error("useColorMode must be used within a ColorModeProvider");
  return ctx;
}