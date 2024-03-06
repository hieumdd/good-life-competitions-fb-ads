import { BigQuery } from '@google-cloud/bigquery';

import { getLogger } from '../logging.service';

const logger = getLogger(__filename);

export const bigqueryClient = new BigQuery();

type CreateExternalTableOptions = {
    name: string;
    sourceUris: string[];
    schema: any[];
    sourceUriPrefix?: string;
};

export const createExternalTable = async (dataset: string, options: CreateExternalTableOptions) => {
    const { name: tableName, sourceUris, schema, sourceUriPrefix } = options;

    const table = bigqueryClient.dataset(dataset).table(tableName);
    if (await table.exists().then(([response]) => response)) {
        logger.debug(`Replacing table ${table.id}`);
        await table.delete();
    }
    await table.create({
        schema,
        externalDataConfiguration: {
            sourceUris,
            sourceFormat: 'NEWLINE_DELIMITED_JSON',
            ignoreUnknownValues: true,
            hivePartitioningOptions: sourceUriPrefix ? { mode: 'CUSTOM', sourceUriPrefix } : undefined,
        },
    });
    logger.debug(`Table ${table.id} created`);
};
