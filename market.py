import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получить информацию о размещённых товарах на Яндекс.Маркете.

    Args:
        page (str): Идентификатор страницы c результатами. Если не указан,
                    возвращается самая старая страница.
        campaign_id (int): Идентификатор кампании/магазина.
        access_token (str): Токен доступа к API Яндекс.Маркета.

    Return:
        dict: Информация о товарах в каталоге и пагинация.

    Examples:
        > get_product_list("<PAGE_TOKEN>", "<CAMPAIGN>", "<API_TOKEN>")
        {
            "paging": {...},
            "offerMappingEntries": [{"offer": {...}, ...}, ...]
        }

    .. _Список товаров в каталоге
        https://yandex.ru/dev/market/partner-api/doc/ru/reference/offer-mappings/getOfferMappingEntries
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Обновить остатки товаров в магазине Яндекс.Маркета.

    Args:
        stocks (list of dict): Данные об остатках товаров.
        campaign_id (int): Идентификатор кампании/магазина.
        access_token (str): Токен доступа к API Яндекс.Маркета.

    Returns:
        dict: Статус выполнения запроса.

    Examples:
        > update_stocks([{"sku": 12345, "items": [...], ...}, ...],
                        "<CAMPAIGN>", "<API_TOKEN>")
        {"status": "OK"}

        > update_stocks([{"sku": <BAD_SKU>, "items": [...], ...}, ...],
                        "<CAMPAIGN>", "<API_TOKEN>")
        {
            "status": "OK",
            "errors": [{"code": "string", "message": "string"}]
        }
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Установить цены на товары в магазине Яндекс.Маркета.

    Args:
        prices (list of dict): Данные о ценах товаров.
        campaign_id (int): Идентификатор кампании/магазина.
        access_token (str): Токен доступа к API Яндекс.Маркета.

    Returns:
        dict: Статус выполнения запроса.

    Examples:
        > update_price([{"id": "12345", "price": {"value": 123, "currencyId": "RUR"}}, ...],
                       "<CAMPAIGN>", "<API_TOKEN>")
        {"status": "OK"}

        > update_price(<BAD_PRICES>, "<CAMPAIGN>", "<API_TOKEN>")
        {
            "status": "OK",
            "errors": [{"code": "string", "message": "string"}]
        }
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получить артикулы товаров Яндекс.Маркета.

    Args:
        campaign_id (int): Идентификатор кампании/магазина.
        access_token (str): Токен доступа к API Яндекс.Маркета.

    Returns:
        list of str: Список артикулов товаров, размещённых на Маркете.

    Examples:
        > get_offer_ids("<CAMPAIGN>", "<API_TOKEN>")
        ["123", "234", ...]

        > get_offer_ids("<BAD_CAMPAIGN>", "<OR_BAD_API_TOKEN>")
        {
            "status": "OK",
            "errors": [{"code": "string", "message": "string"}]
        }
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Вернуть актуальные остатки для обновления товаров на Маркете.

    Args:
        watch_remnants (list of dict): Текущие остатки товаров.
        offer_ids (list of str): Товары, размещённые на Маркете.
        warehouse_id (int): Идентификатор склада.

    Returns:
        list of dict: Список словарей с остатками для размещённых позиций.

    Examples:
        > create_stocks([{"Код": 69785, "Количество": 4, ...}, ...],
                        ["1234", "2345", ...], <WAREHOUSE_ID>)
        [{"sku": "1234", "items": [...]}, ...]
    """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Вернуть актуальные цены для обновления товаров на Маркете.

    Args:
        watch_remnants (list of dict): Текущие остатки товаров.
        offer_ids (list of str): Товары, размещённые на Маркете.

    Returns:
        list of dict: Список словарей с ценами для размещённых позиций.

    Examples:
        > create_prices([{"Код": 69785, "Количество": 4, ...}, ...]
                        ["1234", "2345", ...])
        [{"id": "12345", "price": {"value": 123, "currencyId": "RUR"}}, ...]
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Обновляет цены размещённых на Маркете товаров из текущих остатков.

    Args:
        watch_remnants (list of dict): текущие остатки товаров.
        campaign_id (str): Идентификатор кампании/магазина.
        market_token (str): API-токен продавца на Маркете.

    Returns:
        list of dict: Список обновленных цен.

    Examples:
        > upload_prices([{"Код": 69785, "Количество": 4, ...}, ...],
                        "<CAMPAIGN_ID>", "<TOKEN>")
        [{"id": "12345", "price": {"value": 123, "currencyId": "RUR"}}, ...]
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """Обновляет остатки размещённых на Маркете товаров из текущих остатков.

    Args:
        watch_remnants (list of dict): текущие остатки товаров.
        campaign_id (str): Идентификатор кампании/магазина.
        market_token (str): API-токен продавца на Маркете.
        warehouse_id (int): Идентификатор склада.

    Returns:
        tuple of (list of dict, list of dict): Пару значений: остатки с нулевым
        и ненулевым количеством.

    Examples:
        > upload_stocks([{"Код": 69785, "Количество": 4, ...}, ...],
                        "<CAMPAIGN_ID>", "<TOKEN>", <WAREHOUSE_ID>)
        [{"sku": "1234", "items": [...]}, ...]
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
