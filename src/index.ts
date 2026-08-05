import { registerPlugin } from '@capacitor/core';

import type { IBastionNetworkPlugin } from './definitions';

export const BastionNetworkPlugin = registerPlugin<IBastionNetworkPlugin>('BastionNetworkPlugin');

export * from './definitions';
