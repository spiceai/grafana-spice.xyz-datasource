import { DataQuery, DataSourceJsonData } from '@grafana/data';

export type QuerySource = 'default' | 'firecache';

export interface MyQuery extends DataQuery {
  querySource?: QuerySource;
  queryText?: string;
}

export const DEFAULT_QUERY: Partial<MyQuery> = {
  querySource: 'default',
  queryText: 'SELECT * FROM eth.recent_blocks LIMIT 10',
};

/**
 * These are options configured for each DataSource instance
 */
export interface MyDataSourceOptions extends DataSourceJsonData {}

/**
 * Value that is used in the backend, but never sent over HTTP to the frontend
 */
export interface MySecureJsonData {
  apiKey?: string;
}
