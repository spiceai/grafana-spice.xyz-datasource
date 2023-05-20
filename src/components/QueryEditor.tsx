import React from 'react';
import { CodeEditor } from '@grafana/ui';
import { QueryEditorProps } from '@grafana/data';
import { DataSource } from '../datasource';
import { MyDataSourceOptions, MyQuery } from '../types';

type Props = QueryEditorProps<DataSource, MyQuery, MyDataSourceOptions>;

export function QueryEditor({ query, onChange, onRunQuery }: Props) {
  const onQueryTextChange = (value: string) => {
    onChange({ ...query, queryText: value });
  };

  const { queryText } = query;

  return (
    <CodeEditor
      width={600}
      height={200}
      language="sql"
      showMiniMap={false}
      onEditorDidMount={() => {}} // hack - onChange won't work without this
      onSave={onQueryTextChange}
      onChange={onQueryTextChange}
      onBlur={onRunQuery}
      value={queryText || ''}
    />
  );
}
