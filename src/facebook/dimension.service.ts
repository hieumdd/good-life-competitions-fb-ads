import { getClient, getExtractStream } from './api.service';
import { FacebookRequestOptions } from '../pipeline/pipeline.request.dto';

type GetDimensionsConfig = {
    endpoint: string;
    fields: string[];
};

export const getDimensionStream = ({ endpoint, fields }: GetDimensionsConfig) => {
    return async ({ accountId }: FacebookRequestOptions) => {
        const client = await getClient();

        return getExtractStream(client, (after) => ({
            method: 'GET',
            url: `/act_${accountId}/${endpoint}`,
            params: { fields, limit: 250, after },
        }));
    };
};
