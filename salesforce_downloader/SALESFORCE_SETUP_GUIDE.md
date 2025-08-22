# Salesforce Commerce Cloud Integration: Required Credentials

To enable LiveChannel to download product and order data from your Salesforce Commerce Cloud (SFCC) instance, we require the credentials and information listed below. This guide provides step-by-step instructions on how you can generate and locate the necessary details within your SFCC environment.

## Summary of Required Information

Please provide LiveChannel with the following information:

1.  **API Client ID**
2.  **API Client Secret**
3.  **Your Organization ID**
4.  **Your Site ID**
5.  **Your Base API URL** (including your unique short code)
6.  **Your Catalog ID**
7.  **Your Price Book ID**

---

## Step-by-Step Guide

### Step 1: Create an API Client in Your Account Manager

To allow LiveChannel's application to communicate with your SFCC instance, you need to create a dedicated API Client. This will generate the **Client ID** and **Client Secret** that you will provide to us.

1.  **Log in to your Salesforce Commerce Cloud Account Manager.** This is the top-level account management tool, separate from Business Manager.
2.  Navigate to **API Client** > **Add API Client**.
3.  Enter a descriptive **Display Name** for the client (e.g., "LiveChannel Data Extractor").
4.  Enter a **Password** and confirm it. This password is for managing the API client within your system.
5.  Under **Organizations**, select the organization you want to grant access to.
6.  Under **Roles**, assign the **Salesforce Commerce API** role.
7.  Under **Allowed Scopes**, you must add the specific permissions LiveChannel's application needs. Click **Add** and enter the following scopes one by one:

    ```text
    sfcc.shopper-products.readonly
    sfcc.shopper-orders.readonly
    sfcc.pricing.shopper-pricing
    sfcc.shopper-myaccount.readonly
    ```

8.  Click **Save**. You will now be presented with the **API Client ID** and **API Client Secret**. 
    *   **Important**: Copy the **Client Secret** immediately and store it securely to provide to LiveChannel. You will not be able to see it again after you leave this screen.

### Step 2: Locate Your Organization and Site Details

These identifiers tell our application which specific part of your SFCC environment to target.

1.  **Log in to your Salesforce Commerce Cloud Business Manager.**
2.  **Find your Organization ID**: 
    *   Navigate to **Administration** > **Site Development** > **Salesforce Commerce API Settings**.
    *   Your **Organization ID** is listed here. It typically follows a format like `f_ecom_yourorg_001`.
3.  **Find your Site ID**:
    *   Navigate to **Administration** > **Sites** > **Manage Sites**.
    *   The **ID** column lists all your available Site IDs. Please provide the ID for the specific site (storefront) from which we need to pull data from the LiveChannel store containing your products.
4.  **Determine your Base API URL**:
    *   This is the primary URL for API requests. It is composed of your organization's **short code**.
    *   The format is `https://<short_code>.api.commercecloud.salesforce.com`.
    *   You can find your short code in the URL when you are logged into Business Manager or in your initial SFCC setup documentation.

### Step 3: Identify Your Catalog ID

To ensure we pull products from the correct catalog, please provide the ID of the master or storefront catalog you want us to use.

1.  In **Business Manager**, navigate to **Merchant Tools** > **Products and Catalogs** > **Catalogs**.
2.  A list of your catalogs will be displayed. Please provide the **ID** of the catalog that should be used for product data extraction from the LiveChannel store containing your products (e.g., `your-storefront-catalog`).

### Step 4: Identify Your Price Book ID

We need to know which price book to query for product pricing information.

1.  In **Business Manager**, navigate to **Merchant Tools** > **Products and Catalogs** > **Price Books**.
2.  A list of your price books will be displayed. Please provide the **ID** of the price book that contains the list prices for the LiveChannel store containing your products (e.g., `your-site-list-prices`).

---

## Providing the Information Securely

Once you have gathered all the information, please share it with LiveChannel through a secure channel, such as an encrypted message or a password-protected document. Do not send these credentials over unsecured email.

Thank you!
