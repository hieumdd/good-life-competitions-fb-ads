import { Readable } from 'node:stream';
import axios, { AxiosInstance, AxiosRequestConfig } from 'axios';
import qs from 'query-string';

import { getLogger } from '../logging.service';

const logger = getLogger(__filename);

export const getClient = async () => {
    const accessToken = await axios
        .request<{ value: { raw: string } }>({
            method: 'GET',
            url: 'https://api.doppler.com/v3/configs/config/secret',
            params: { project: 'facebook', config: 'master', name: 'USER_ACCESS_TOKEN' },
            auth: { username: process.env.DOPPLER_TOKEN ?? '', password: '' },
        })
        .then(({ data }) => data.value.raw);

    const apiVersion = await axios
        .request({
            method: 'GET',
            url: 'https://graph.facebook.com/me',
            params: { access_token: accessToken },
        })
        .then((response) => <string>response.headers['facebook-api-version']);

    const client = axios.create({
        baseURL: `https://graph.facebook.com/${apiVersion}`,
        params: { access_token: accessToken },
        paramsSerializer: { serialize: (value) => qs.stringify(value, { arrayFormat: 'comma' }) },
    });

    client.interceptors.response.use(
        (response) => response,
        (error) => {
            const meta = axios.isAxiosError(error)
                ? {
                      config: error.config,
                      response: { data: error.response?.data, headers: error.response?.headers },
                  }
                : error;

            logger.error({ error: meta });
            throw error;
        },
    );

    return client;
};

export const getExtractStream = (client: AxiosInstance, config: (after?: string) => AxiosRequestConfig) => {
    const stream = new Readable({ objectMode: true, read: () => {} });

    const _get = (after?: string) => {
        type GetResponse = {
            data: Record<string, any>[];
            paging?: { cursors: { after: string }; next: string };
        };

        client
            .request<GetResponse>(config(after))
            .then((response) => response.data)
            .then(({ data, paging }) => {
                data.forEach((row) => stream.push(row));
                paging?.next ? _get(paging.cursors.after) : stream.push(null);
            })
            .catch((error) => stream.emit('error', error));
    };

    _get();

    return stream;
};
