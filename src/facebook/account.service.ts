import { getClient } from './api.service';

export const getAccounts = async (businessId: number) => {
    const client = await getClient();

    return await Promise.all(
        ['client_ad_accounts', 'owned_ad_accounts'].map(async (edge) => {
            return await client
                .request<{ data: { account_id: string; id: string; name: string }[] }>({
                    method: 'GET',
                    params: { limit: 500, fields: ['name', 'account_id'] },
                    url: `/${businessId}/${edge}`,
                })
                .then((response) => {
                    return response.data.data.map((row) => ({
                        account_id: row.account_id,
                        account_name: row.name,
                    }));
                });
        }),
    ).then((accountGroups) => accountGroups.flat());
};
