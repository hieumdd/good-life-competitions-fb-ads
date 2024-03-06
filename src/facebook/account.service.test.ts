import { getAccounts } from './account.service';

it('getAccounts', async () => {
    return await getAccounts(1804102293183102)
        .then((result) => {
            expect(result).toBeDefined();
        })
        .catch((error) => {
            console.error({ error });
            throw error;
        });
});
