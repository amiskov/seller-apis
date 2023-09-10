"""Работа с Ozon Seller API https://docs.ozon.ru/api/seller/"""
import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получить список товаров магазина Озон.

    Args:
        last_id (str): Идентификатор последнего значения на странице.
        client_id (str): Идентификатор продавца.
        seller_token (str): API-токен продавца.

    Returns:
        dict: Возвращает список пар значений offer_id и product_id. За один запрос
        значений может быть <= 1000.

    Examples:
        # Первый запрос, last_id неизвестен, оставляем пустым:
        > get_product_list('', '<CLIENT_ID>', '<TOKEN>')
        {"items": [{"product_id": 223681945, "offer_id": "136748"}, ...]}

        # Используем полученный last_id для следующего запроса:
        > get_product_list('bnVсbA==', '<CLIENT_ID>', '<TOKEN>')
        {"items": [{"product_id": 12345, "offer_id": "23455"}, ...]}

    .. _Ozon Seller API:
        https://docs.ozon.ru/api/seller/#operation/ProductAPI_GetProductList
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получить артикулы товаров, размещённых на Озоне.

    Args:
        client_id (str): Идентификатор продавца.
        seller_token (str): API-токен продавца.

    Returns:
        list of str: Список артикулов товаров (offer_id), опубликованных в Озоне.

    Examples:
        > get_offer_ids("<CLIENT_ID>", "<TOKEN>")
        ["136748", "136749", ...]
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновить цены товаров, размещённых на Озоне.

    Args:
        prices (list of str): Цены товаров для обновления.
        client_id (str): Идентификатор продавца.
        seller_token (str): API-токен продавца.

    Returns:
        dict: Ответ сервера после обновления цен.

    Example:
        > update_prices(
            [{
              "auto_action_enabled": "UNKNOWN",
              "currency_code": "RUB",
              "offer_id": "",
              "old_price": "0",
              "price": "1448",
            }, ... ],
            "<CLIENT_ID>",
            "<TOKEN>"
        )

        {"result": [{
            "product_id": 1386,
            "offer_id": "PH8865",
            "updated": true,
            "errors": []
        }]}

    .. _Ozon Seller API:
        https://docs.ozon.ru/api/seller/#operation/ProductAPI_ImportProductsPrices
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновить остатки товаров, размещённых на Озоне.

    Args:
        stocks (list of str): Остатки товаров для обновления.
        client_id (str): Идентификатор продавца.
        seller_token (str): API-токен продавца.

    Returns:
        dict: Ответ сервера после обновления остатков.

    Examples:
        > update_stocks(
            [{
                  "offer_id": "PG-2404С1",
                  "product_id": 55946,
                  "stock": 4
            }, ...],
            "<CLIENT_ID>",
            "<TOKEN>")

        {"result": [{
            "product_id": 55946,
            "offer_id": "PG-2404С1",
            "updated": true,
            "errors": []
        }]}

    .. _Ozon Seller API:
        https://docs.ozon.ru/api/seller/#operation/ProductAPI_ImportProductsStocks
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Вернуть остатки с сайта Casio в виде словаря.

    Скачивает Excel-таблицу и преобразует её в словарь.

    Returns:
        dict: Остатки товаров с ценами и количеством.

    Examples:
        > download_stock()
        [{
            "Код": 69785,
            "Количество": 4,
            "Цена": "5'990.00 руб.",
            ...
        }, ...]
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Вернуть актуальные остатки для товаров на Озоне.

    Args:
        watch_remnants (list of dict): текущие остатки товаров.
        offer_ids (list of str): товары, размещённые на Озоне.

    Returns:
        list of dict: Список словарей с остатками для размещённых позиций.

    Examples:
        > create_stocks([{"Код": 69785, "Количество": 4, ...}, ...]
                        ["136748", "136749", ...])
        [{"offer_id": "136748", "stock": 3}, ...]
    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Вернуть актуальные цены для товаров на Озоне.

    Args:
        watch_remnants (list of str): текущие остатки товаров.
        offer_ids (list of str): товары, размещённые на Озоне.

    Returns:
        list of dict: Список словарей с актуальными ценами для размещённых на
                      Озоне товаров.

    Examples:
        > create_prices([{"Код": 69785, "Цена": "5'990.00 руб.", ...}, ...]
                        ["136748", "136749", ...])
        [{
          "auto_action_enabled": "UNKNOWN",
          "currency_code": "RUB",
          "offer_id": "136748",
          "old_price": "0",
          "price": "5990",
        }, ... ]
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразовать цену в строку с целым числом.

    Args:
        price (str): Строка с ценой, из которой нужно убрать лишние символы.

    Returns:
        str: Строка с ценой в виде целого числа без копеек и прочих символов.

    Examples:
        >>> price_conversion("5'990.00 руб.")
        '5990'
        >>> price_conversion("5990")
        '5990'
        >>> price_conversion("руб.")
        ''
        >>> price_conversion(5990)
        Traceback (most recent call last):
        ...
        AttributeError: 'int' object has no attribute 'split'
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделить список lst на части по n элементов.

    Examples:
        >>> divided_gen = divide([1, 2, 3], 2)
        >>> next(divided_gen)
        [1, 2]
        >>> next(divided_gen)
        [3]
        >>> next(divided_gen)
        Traceback (most recent call last):
        ...
        StopIteration
    """
    for i in range(0, len(lst), n):
        yield lst[i: i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Обновляет цены размещённых на Озоне товаров из текущих остатков.

    Args:
        watch_remnants (list of dict): текущие остатки товаров.
        client_id (str): Идентификатор продавца.
        seller_token (str): API-токен продавца.

    Returns:
        list of dict: Список словарей для обновлённых цен.

    Examples:
        > upload_prices(
            [{
                "Код": 69785,
                "Количество": 4,
                "Цена": "5'990.00 руб.",
                ...
            }, ...
            ], "<CLIENT_ID>", "<TOKEN>")

        [{
          "auto_action_enabled": "UNKNOWN",
          "currency_code": "RUB",
          "offer_id": "",
          "old_price": "0",
          "price": "1448",
        }, ... ]
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Обновляет количество размещённых на Озоне товаров из текущих остатков.

    Args:
        watch_remnants (list of dict): текущие остатки товаров.
        client_id (str): Идентификатор продавца.
        seller_token (str): API-токен продавца.

    Returns:
        tuple of (list of dict, list of dict): Пару значений: остатки с нулевым
        и ненулевым количеством.

    Examples:
        > upload_prices(
            [{
                "Код": 69785,
                "Количество": 4,
                "Цена": "5'990.00 руб.",
                ...
            }, ...
            ], "<CLIENT_ID>", "<TOKEN>")

        [{
          "auto_action_enabled": "UNKNOWN",
          "currency_code": "RUB",
          "offer_id": "",
          "old_price": "0",
          "price": "1448",
        }, ... ],
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
