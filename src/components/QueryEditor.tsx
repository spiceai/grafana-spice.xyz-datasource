import React, { useEffect, useState } from 'react';
import { CodeEditor, Field, RadioButtonGroup } from '@grafana/ui';
import { QueryEditorProps, SelectableValue } from '@grafana/data';
import { DataSource } from '../datasource';
import { MyDataSourceOptions, MyQuery, QuerySource } from '../types';

type Props = QueryEditorProps<DataSource, MyQuery, MyDataSourceOptions>;

const sourceOptions: Array<SelectableValue<QuerySource>> = [
  { label: 'Spice.ai', value: 'default' },
  { label: 'Firecache', value: 'firecache', icon: 'fire' },
];

export function QueryEditor({ query, onChange, onRunQuery, datasource, app }: Props) {
  const [firecacheAvailable, setFirecacheAvailable] = useState(false);

  const onQueryTextChange = (value: string) => {
    onChange({ ...query, queryText: value });
  };

  const onQuerySourceChange = (value: MyQuery['querySource']) => {
    onChange({ ...query, querySource: value });
  };

  useEffect(() => {
    datasource
      .getResource('datasets')
      .then((datasets: any[]) => {
        if (datasets?.length > 0) {
          setFirecacheAvailable(true);
        } else {
          onQuerySourceChange('default');
        }
      })
      .catch((e) => {
        onQuerySourceChange('default');
      });

    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [app, datasource]);

  const { queryText, querySource: queryType } = query;

  return (
    <div>
      <Field label="Source">
        <RadioButtonGroup
          options={sourceOptions}
          value={queryType}
          onChange={onQuerySourceChange}
          disabledOptions={firecacheAvailable ? [] : ['firecache']}
        />
      </Field>

      <Field label="Query">
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
