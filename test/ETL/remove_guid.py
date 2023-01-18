import boto3
import logging
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime
from botocore.exceptions import ClientError
import time as _time

# for local testing use actual table
# for lambda execution runtime use dynamic reference
TABLE_NAME = "rtcwprostats-database-DDBTable2F2A2F95-1BCIOU7IE3DSE"

dynamodb = boto3.resource('dynamodb')
ddb_table = dynamodb.Table(TABLE_NAME)
ddb_client = boto3.client('dynamodb')

log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger()
logger.setLevel(log_level)

log_stream_name = "local"


def delete_guid(ddb_table, guid):
    pk_base = "player#"
    pk = pk_base + guid
    response = ddb_table.query(KeyConditionExpression=Key('pk').eq(pk))

    delete_items = []
    if response['Count'] > 0:
        for record in response["Items"]:

            if "achievement" in record['sk'] or "aggstats" in record['sk'] or "aggwstats" in record['sk'] or "elo#" in record['sk']:
                delete_items.append(record)
            else:
                logger.info("Keeping " + record['sk'])

        logger.info("deleting " + str(len(delete_items)).zfill(2) + " guid items for " + guid)

        with ddb_table.batch_writer() as batch:
            for each in delete_items:
                batch.delete_item(
                    Key={key: each[key] for key in ['pk','sk']}
                )



if __name__ == "__main__":
    elsas = [
        "6b8d7958063c4a3000e22e70da6fc6f0",
        "acd78338113a58979df38630ae4f3276",
        "1a38213b4ce45c93ede97e8ff9256f04",
        "e12d6a025006c2a357984c74ea1dea4d",
        "6b8d7958063c4a3000e22e70da6fc6f0",
        "90a72c199ba95e2150e8a765bddbee23",
        "8542cff3662eebd5e069bece19b133b3",
        "7b7ae19003c8d89ab67df68830ea46b7",
        "03df0f1323f908bb358d64a4d5cfafc3",
        "4753853de3d6cb25f1c2954e120773cd",
        "e8679688e9660b8b0c3ca10f6c2b2b21",
        "6d2ecc4f8aecce600fdfbca9367da0ed",
        "3dd50b100bff1fd3de39d637d5a6b2f6",
        "f4957df751554f08cd61a3986ff005b1",
        "32dfadb6a7a90c7cbecfddcefeb12180",
        "3a498747d08d9641e95f075272f53cd9",
        "8e1082bc8a7e6d881e71c3d1b870a2f6",
        "b2ad9e8d4cf464acea166c1927372731",
        "f4087c553dc0abece2bf4b7d30b258ad",
        "ab122e70e4fb19414e3214dce59c74ef",
        "346ce9b7d481845eaf50680817c58ae1",
        "e812ea5d5428b5304900e24bbdc00d58",
        "82f1dec33ae8ca7f35a5599ab2f75c1d",
        "80db9256ca8631c38dd6bf0c991b444b",
        "20c37ca9ad882eff6f544164b91238e2",
        "2a4677af242ec70b44b85a4ce8f4baff",
        "d1f38c8f669861aeedaee21812dacc53",
        "af3949abee79835a08147c3397e9bcbf",
        "3dd50b100bff1fd3de39d637d5a6b2f6",
        "d1f38c8f669861aeedaee21812dacc53",
        "ab122e70e4fb19414e3214dce59c74ef",
        "f4087c553dc0abece2bf4b7d30b258ad",
        "e812ea5d5428b5304900e24bbdc00d58",
        "32dfadb6a7a90c7cbecfddcefeb12180",
        "af3949abee79835a08147c3397e9bcbf",
        "29ae0fa73b49b79805c0442238c98038",
        "c56cb120f83321e113d3dafdf02b7717",
        "90a72c199ba95e2150e8a765bddbee23",
        "9cc5f200a83ab04e475234bbc7a50674",
        "d01b96d4ccd950554e77d78164c72127",
        "eadf4df0b353d72f2257020111315cc1",
        "91594542d328fa9bd590a220de77a7f2",
        "68d97a443fc338dea028d9c16818746f",
        "899120ca8260b6f3ed4d7be610ef8522",
        "63dfc4ea9b314bd565ba98224c5de0a3",
        "89eb91083813eb87af3627d1d59751a0",
        "d5a73c5f4ca57abe5b3acdc6aeaa08d8",
        "4bff685056a53e58d68b42c10153b0ed",
        "4f901041b0cb8e7728c412e0ef268712",
        "5e14f0d833d11ad2ac97d33f012cafaf",
        "9720bd47409ed02760c37688cd341074",
        "6a34b106975876157ab5bbeeeb8bd905",
        "377ecf9194296e68a055fa15ae02a3e5",
        "43fa63de1dbadbf203c05dfb0c5c376a",
        "d5a73c5f4ca57abe5b3acdc6aeaa08d8",
        "9ddb8b3fe6d39e9804aa40c890ce7fc9",
        "43fa63de1dbadbf203c05dfb0c5c376a",
        "91594542d328fa9bd590a220de77a7f2",
        "9720bd47409ed02760c37688cd341074",
        "89eb91083813eb87af3627d1d59751a0",
        "6a34b106975876157ab5bbeeeb8bd905",
        "899120ca8260b6f3ed4d7be610ef8522",
        "b84e7981497c25446c487c55602b9b96",
        "8b3540911915f24a1e42cc5f2f6a1166",
        "8e865a6014c7fad1369e34d987af7e25",
        "e1f529dcfa002648bd3a53f729a43bd3",
        "de346527d35dc3c3572c3ff6f66a2002",
        "12d189f8a112087cbdff395e0336043a",
        "28966de57a4d5db5790e7bd14326053e",
        "2897235d4a870daffd68932b37061d88",
        "31ec2f2d719b268a63ef7b1e4ed10ce0",
        "dd1db8a57bdd21474389fd81b24b0a3a",
        "7b02c6385a59d154e03afcb67991e489",
        "c984be8a384cdbabbac7fd8504c8fc7d",
        "4204670597fe4e13f0811733c362a6b7",
        "89c4fecdc290bc96477a4dd961178733",
        "2f08ec5554a8001e20f44f9726fe535a",
        "b463a13d23469e2b548b3ba222bab9fd",
        "fc1f1285b8045a1ae3bfdbff55db6766",
        "2c0a253b25e3d9fb8cf53108ce1c65a3",
        "36738bdae9bbb3449935e9b1db4d3bc7",
        "939c000ceac264e31727ab721b93844f",
        "7c03b19e32132d62486a68b9f5663f84",
        "82bd3d5c0eccab9d2d5faa604d18c58a",
        "1bb020198859729cd82a2d230cfc2215",
        "472e7d1fb3d80c66d67f5a3990e2706c",
        "0abc541267acbce29de863793897e0a8",
        "c6e46d42538b1596c6cc96904129ba0f",
        "e5871544467360db33da1a6e66586b3d",
        "9f8f6eadd2ab686b9523173c8a84c0c5",
        "31ec2f2d719b268a63ef7b1e4ed10ce0",
        "e1f529dcfa002648bd3a53f729a43bd3",
        "8e865a6014c7fad1369e34d987af7e25",
        "c984be8a384cdbabbac7fd8504c8fc7d",
        "98c3ecb455a9eb0bb4d0628fff4d7d50",
        "939c000ceac264e31727ab721b93844f",
        "de346527d35dc3c3572c3ff6f66a2002",
        "2c0a253b25e3d9fb8cf53108ce1c65a3",
        "4204670597fe4e13f0811733c362a6b7",
        "744975e55507a46f14a93b929123b4ed",
        "82bd3d5c0eccab9d2d5faa604d18c58a",
        "5c3c20ea93aa994bfc2512ec64bf86c0",
        "1bb020198859729cd82a2d230cfc2215",
        "7c03b19e32132d62486a68b9f5663f84",
        "b463a13d23469e2b548b3ba222bab9fd",
        "76f4a81a4997095edcc6d5321eda17c3",
        "bc96ec9eeed915118987f6040ea77d64",
        "7c55e8157662d9516622d202680419a3",
        "51ee75ef0e955bf81cd6713a28ec3037",
        "f8021e2472d9aa9418fab73263b34c92",
        "f8021e2472d9aa9418fab73263b34c92",
        "58fe3b618217c2bf47f31d3a4592afe0",
        "94063ca4afb7cc29663ca4f496fd9de7",
        "95c4a4d76270c6a3fe72f920b10236e3",
        "a2ccdd6ca11d04c27aba79a0c50d96f2",
        "24b9b409256813ffb4e23b9742ebccdb",
        "20ee2ad847d076986dd1e35a79413b3f",
        "98cd6b60cab9a3cf149773414ff26468",
        "a434675d98477c6eb1b28a15e84774da",
        "a2ccdd6ca11d04c27aba79a0c50d96f2",
        "932e24627360902993db416a9446ac5a",
        "ea6be9699d58e7d923bdca2fbcf12e7f",
        "bfdbf4369c495bd484e65d04af3187eb",
        "98aa9e675bce5f14ce6559e4e47520b1",
        "f24bc38f567b854e0f35d9a62ebcbbe9",
        "e847163c11f4472dc32a01fc2d44c730",
        "ea6be9699d58e7d923bdca2fbcf12e7f",
        "f24bc38f567b854e0f35d9a62ebcbbe9",
        "02adc0f632406429821b2fb47264e292",
        "0c565d9e0428248118e7436bbf1897d3",
        "61057bf417d1d203e2943330df9fc43a",
        "c3c1e16569fecaaf287ab0766979aa68",
        "85c9c2f0774c7445da2ee2e153cd813a",
        "85c9c2f0774c7445da2ee2e153cd813a",
        "9d7639fc2eb14ef253c3af7428a58476",
        "033de01df8f21ef1f1d6faaf28993630",
        "78de6b9c8d61b219f5e8faa07318c61b",
        "69444420156c8061ef1d2e05d87f483d",
        "0db36c4764651b00dd074d3af0d1f036",
        "1906988f02757b5d9df998d5eb40d699",
        "b747aa40ad31bfda444e608388e655c0",
        "e4613ae03747f4dfafefaab071fe29aa",
        "800407976ec0378190c3131270381ce6",
        "aa942fc026b2419553d6b592e5c1854c",
        "5d375b8d5522e4a057a42ade901a9de7",
        "cdd23ca1bd5eb9e227ca8d7f168de285",
        "b747aa40ad31bfda444e608388e655c0",
        "b16be7f34e9d90a8c3b2a4ab5e192f08",
        "c4cd42fd9907e58d8753ae04fa2acb34",
        "0db36c4764651b00dd074d3af0d1f036",
        "0c981ba03024641e0b7da8e7412a82c4",
        "1906988f02757b5d9df998d5eb40d699",
        "69444420156c8061ef1d2e05d87f483d"
    ]
    for guid in elsas:
        delete_guid(ddb_table, guid)

    for guid in elsas:
        elsa_fake = "elsa.fake"
        expression_values = {':gsi1sk': "realname#" + elsa_fake, ':data': elsa_fake}
        update_expression = "set #data_value = :data, gsi1sk = :gsi1sk"
        key = {"pk": "player#" + guid, "sk": "realname"}

        response = ddb_table.update_item(Key=key, UpdateExpression=update_expression, ExpressionAttributeValues=expression_values, ExpressionAttributeNames={"#data_value": "data"})


