from src.shopee_affiliates_client import ShopeeAffiliatesClient

QUERY = """
mutation generateShortLink($url: String!) {
  generateShortLink(input: { originUrl: $url }) {
    shortLink
  }
}
"""

def main():
    client = ShopeeAffiliatesClient.from_env()

    result = client.execute(
        QUERY,
        variables={
            "url": "https://shopee.com.br/Apple-iPhone-11-128GB-Local-Set-i.52377417.6309028319"
        }
    )

    print(result)

if __name__ == "__main__":
    main()
