import React, { ChangeEvent } from 'react';
import { InlineField, SecretInput, Input } from '@grafana/ui';
import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { MyDataSourceOptions, MySecureJsonData } from '../types';

interface Props extends DataSourcePluginOptionsEditorProps<MyDataSourceOptions> {}

export function ConfigEditor(props: Props) {
  const { onOptionsChange, options } = props;

  // Secure field (only sent to the backend)
  const onAPIKeyChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({
      ...options,
      secureJsonData: {
        apiKey: event.target.value,
      },
    });
  };

  const onFlightAddressChange = (event: ChangeEvent<HTMLInputElement>) => {
    const jsonData = {
      ...options.jsonData,
      flightAddress: event.target.value,
    };
    onOptionsChange({ ...options, jsonData });
  };

  const onResetAPIKey = () => {
    onOptionsChange({
      ...options,
      secureJsonFields: {
        ...options.secureJsonFields,
        apiKey: false,
      },
      secureJsonData: {
        ...options.secureJsonData,
        apiKey: '',
      },
    });
  };

  const { secureJsonFields, jsonData } = options;
  const secureJsonData = (options.secureJsonData || {}) as MySecureJsonData;

  return (
    <div className="gf-form-group">
      <InlineField
        label="Spice Flight Address"
        labelWidth={24}
        required
        tooltip="Spice.xyz Apache Arrow Flight gRPC address"
      >
        <Input
          value={jsonData.flightAddress || 'flight.spiceai.io:443'}
          width={40}
          placeholder="flight.spiceai.io:443"
          defaultValue="flight.spiceai.io:443"
          onChange={onFlightAddressChange}
        />
      </InlineField>
      <InlineField label="Spice API Key" labelWidth={24} required>
        <SecretInput
          required
          isConfigured={(secureJsonFields && secureJsonFields.apiKey) as boolean}
          value={secureJsonData.apiKey || ''}
          placeholder="Spice API Key"
          width={40}
          onReset={onResetAPIKey}
          onChange={onAPIKeyChange}
        />
      </InlineField>
    </div>
  );
}
