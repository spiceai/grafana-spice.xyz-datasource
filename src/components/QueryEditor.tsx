import React from 'react';
import { CodeEditor, Field, RadioButtonGroup } from '@grafana/ui';
import { QueryEditorProps, SelectableValue } from '@grafana/data';
import { DataSource } from '../datasource';
import { MyDataSourceOptions, MyQuery, QuerySource } from '../types';

type Props = QueryEditorProps<DataSource, MyQuery, MyDataSourceOptions>;

const sourceOptions: Array<SelectableValue<QuerySource>> = [
  { label: 'Default', value: 'default' },
  { label: 'Firecache', value: 'firecache', icon: 'fire' },
];

export function QueryEditor({ query, onChange, onRunQuery }: Props) {
  const onQueryTextChange = (value: string) => {
    onChange({ ...query, queryText: value });
  };

  const onQueryTypeChange = (value: MyQuery['querySource']) => {
    onChange({ ...query, querySource: value });
  };

  const { queryText, querySource: queryType } = query;

  return (
    <div>
      <Field label="source">
        <RadioButtonGroup options={sourceOptions} value={queryType} onChange={onQueryTypeChange} />
      </Field>

      <Field label="query">
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
      </Field>
    </div>
  );
}
