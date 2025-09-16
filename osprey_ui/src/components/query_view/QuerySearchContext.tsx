import React, { createContext } from 'react';

export const querySearchContext = createContext<{ query: string; regex: boolean }>({ query: '', regex: false });
