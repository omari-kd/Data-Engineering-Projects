--
-- PostgreSQL database dump
--

\restrict 5X5ChZKhRE7YNKAiI8LchscYAbGJBi0jCFGCm6nxowNIgzPMsxi94ff8u7ndm5r

-- Dumped from database version 16.10 (Ubuntu 16.10-0ubuntu0.24.04.1)
-- Dumped by pg_dump version 16.10 (Ubuntu 16.10-0ubuntu0.24.04.1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

ALTER TABLE ONLY warehouse.fact_sales DROP CONSTRAINT fact_sales_sale_date_fkey;
ALTER TABLE ONLY warehouse.fact_sales DROP CONSTRAINT fact_sales_product_id_fkey;
ALTER TABLE ONLY warehouse.dim_product DROP CONSTRAINT dim_product_category_id_fkey;
ALTER TABLE ONLY warehouse.fact_sales DROP CONSTRAINT fact_sales_pkey;
ALTER TABLE ONLY warehouse.dim_product DROP CONSTRAINT dim_product_pkey;
ALTER TABLE ONLY warehouse.dim_date DROP CONSTRAINT dim_date_pkey;
ALTER TABLE ONLY warehouse.dim_date DROP CONSTRAINT dim_date_date_key;
ALTER TABLE ONLY warehouse.dim_category DROP CONSTRAINT dim_category_pkey;
ALTER TABLE ONLY warehouse.dim_category DROP CONSTRAINT dim_category_category_name_key;
ALTER TABLE warehouse.fact_sales ALTER COLUMN sale_id DROP DEFAULT;
ALTER TABLE warehouse.dim_product ALTER COLUMN product_id DROP DEFAULT;
ALTER TABLE warehouse.dim_date ALTER COLUMN date_id DROP DEFAULT;
ALTER TABLE warehouse.dim_category ALTER COLUMN category_id DROP DEFAULT;
DROP SEQUENCE warehouse.fact_sales_sale_id_seq;
DROP TABLE warehouse.fact_sales;
DROP SEQUENCE warehouse.dim_product_product_id_seq;
DROP TABLE warehouse.dim_product;
DROP SEQUENCE warehouse.dim_date_date_id_seq;
DROP TABLE warehouse.dim_date;
DROP SEQUENCE warehouse.dim_category_category_id_seq;
DROP TABLE warehouse.dim_category;
DROP TABLE public.products;
DROP SCHEMA warehouse;
--
-- Name: warehouse; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA warehouse;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: products; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.products (
    id bigint,
    product_name text,
    unit_price double precision,
    category_name text,
    rating_score double precision,
    rating_count bigint
);


--
-- Name: dim_category; Type: TABLE; Schema: warehouse; Owner: -
--

CREATE TABLE warehouse.dim_category (
    category_id integer NOT NULL,
    category_name character varying(100)
);


--
-- Name: dim_category_category_id_seq; Type: SEQUENCE; Schema: warehouse; Owner: -
--

CREATE SEQUENCE warehouse.dim_category_category_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: dim_category_category_id_seq; Type: SEQUENCE OWNED BY; Schema: warehouse; Owner: -
--

ALTER SEQUENCE warehouse.dim_category_category_id_seq OWNED BY warehouse.dim_category.category_id;


--
-- Name: dim_date; Type: TABLE; Schema: warehouse; Owner: -
--

CREATE TABLE warehouse.dim_date (
    date_id integer NOT NULL,
    date date,
    month integer,
    year integer,
    weekday character varying(20)
);


--
-- Name: dim_date_date_id_seq; Type: SEQUENCE; Schema: warehouse; Owner: -
--

CREATE SEQUENCE warehouse.dim_date_date_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: dim_date_date_id_seq; Type: SEQUENCE OWNED BY; Schema: warehouse; Owner: -
--

ALTER SEQUENCE warehouse.dim_date_date_id_seq OWNED BY warehouse.dim_date.date_id;


--
-- Name: dim_product; Type: TABLE; Schema: warehouse; Owner: -
--

CREATE TABLE warehouse.dim_product (
    product_id integer NOT NULL,
    product_name character varying(200),
    category_id integer,
    brand_name character varying(100)
);


--
-- Name: dim_product_product_id_seq; Type: SEQUENCE; Schema: warehouse; Owner: -
--

CREATE SEQUENCE warehouse.dim_product_product_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: dim_product_product_id_seq; Type: SEQUENCE OWNED BY; Schema: warehouse; Owner: -
--

ALTER SEQUENCE warehouse.dim_product_product_id_seq OWNED BY warehouse.dim_product.product_id;


--
-- Name: fact_sales; Type: TABLE; Schema: warehouse; Owner: -
--

CREATE TABLE warehouse.fact_sales (
    sale_id integer NOT NULL,
    product_id integer,
    quantity integer,
    total_amount numeric(10,2),
    sale_date date
);


--
-- Name: fact_sales_sale_id_seq; Type: SEQUENCE; Schema: warehouse; Owner: -
--

CREATE SEQUENCE warehouse.fact_sales_sale_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: fact_sales_sale_id_seq; Type: SEQUENCE OWNED BY; Schema: warehouse; Owner: -
--

ALTER SEQUENCE warehouse.fact_sales_sale_id_seq OWNED BY warehouse.fact_sales.sale_id;


--
-- Name: dim_category category_id; Type: DEFAULT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.dim_category ALTER COLUMN category_id SET DEFAULT nextval('warehouse.dim_category_category_id_seq'::regclass);


--
-- Name: dim_date date_id; Type: DEFAULT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.dim_date ALTER COLUMN date_id SET DEFAULT nextval('warehouse.dim_date_date_id_seq'::regclass);


--
-- Name: dim_product product_id; Type: DEFAULT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.dim_product ALTER COLUMN product_id SET DEFAULT nextval('warehouse.dim_product_product_id_seq'::regclass);


--
-- Name: fact_sales sale_id; Type: DEFAULT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_sales ALTER COLUMN sale_id SET DEFAULT nextval('warehouse.fact_sales_sale_id_seq'::regclass);


--
-- Data for Name: products; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.products (id, product_name, unit_price, category_name, rating_score, rating_count) FROM stdin;
1	Fjallraven - Foldsack No. 1 Backpack, Fits 15 Laptops	109.95	men's clothing	3.9	120
2	Mens Casual Premium Slim Fit T-Shirts 	22.3	men's clothing	4.1	259
3	Mens Cotton Jacket	55.99	men's clothing	4.7	500
4	Mens Casual Slim Fit	15.99	men's clothing	2.1	430
5	John Hardy Women's Legends Naga Gold & Silver Dragon Station Chain Bracelet	695	jewelery	4.6	400
6	Solid Gold Petite Micropave 	168	jewelery	3.9	70
7	White Gold Plated Princess	9.99	jewelery	3	400
8	Pierced Owl Rose Gold Plated Stainless Steel Double	10.99	jewelery	1.9	100
9	WD 2TB Elements Portable External Hard Drive - USB 3.0 	64	electronics	3.3	203
10	SanDisk SSD PLUS 1TB Internal SSD - SATA III 6 Gb/s	109	electronics	2.9	470
11	Silicon Power 256GB SSD 3D NAND A55 SLC Cache Performance Boost SATA III 2.5	109	electronics	4.8	319
12	WD 4TB Gaming Drive Works with Playstation 4 Portable External Hard Drive	114	electronics	4.8	400
13	Acer SB220Q bi 21.5 inches Full HD (1920 x 1080) IPS Ultra-Thin	599	electronics	2.9	250
14	Samsung 49-Inch CHG90 144Hz Curved Gaming Monitor (LC49HG90DMNXZA) – Super Ultrawide Screen QLED 	999.99	electronics	2.2	140
15	BIYLACLESEN Women's 3-in-1 Snowboard Jacket Winter Coats	56.99	women's clothing	2.6	235
16	Lock and Love Women's Removable Hooded Faux Leather Moto Biker Jacket	29.95	women's clothing	2.9	340
17	Rain Jacket Women Windbreaker Striped Climbing Raincoats	39.99	women's clothing	3.8	679
18	MBJ Women's Solid Short Sleeve Boat Neck V 	9.85	women's clothing	4.7	130
19	Opna Women's Short Sleeve Moisture	7.95	women's clothing	4.5	146
20	DANVOUY Womens T Shirt Casual Cotton Short	12.99	women's clothing	3.6	145
\.


--
-- Data for Name: dim_category; Type: TABLE DATA; Schema: warehouse; Owner: -
--

COPY warehouse.dim_category (category_id, category_name) FROM stdin;
1	men's clothing
2	jewelery
3	electronics
4	women's clothing
\.


--
-- Data for Name: dim_date; Type: TABLE DATA; Schema: warehouse; Owner: -
--

COPY warehouse.dim_date (date_id, date, month, year, weekday) FROM stdin;
1	2025-10-23	10	2025	Thursday
\.


--
-- Data for Name: dim_product; Type: TABLE DATA; Schema: warehouse; Owner: -
--

COPY warehouse.dim_product (product_id, product_name, category_id, brand_name) FROM stdin;
1	Fjallraven - Foldsack No. 1 Backpack, Fits 15 Laptops	1	Generic
2	Mens Casual Premium Slim Fit T-Shirts 	1	Generic
3	Mens Cotton Jacket	1	Generic
4	Mens Casual Slim Fit	1	Generic
5	John Hardy Women's Legends Naga Gold & Silver Dragon Station Chain Bracelet	2	Generic
6	Solid Gold Petite Micropave 	2	Generic
7	White Gold Plated Princess	2	Generic
8	Pierced Owl Rose Gold Plated Stainless Steel Double	2	Generic
9	WD 2TB Elements Portable External Hard Drive - USB 3.0 	3	Generic
10	SanDisk SSD PLUS 1TB Internal SSD - SATA III 6 Gb/s	3	Generic
11	Silicon Power 256GB SSD 3D NAND A55 SLC Cache Performance Boost SATA III 2.5	3	Generic
12	WD 4TB Gaming Drive Works with Playstation 4 Portable External Hard Drive	3	Generic
13	Acer SB220Q bi 21.5 inches Full HD (1920 x 1080) IPS Ultra-Thin	3	Generic
14	Samsung 49-Inch CHG90 144Hz Curved Gaming Monitor (LC49HG90DMNXZA) – Super Ultrawide Screen QLED 	3	Generic
15	BIYLACLESEN Women's 3-in-1 Snowboard Jacket Winter Coats	4	Generic
16	Lock and Love Women's Removable Hooded Faux Leather Moto Biker Jacket	4	Generic
17	Rain Jacket Women Windbreaker Striped Climbing Raincoats	4	Generic
18	MBJ Women's Solid Short Sleeve Boat Neck V 	4	Generic
19	Opna Women's Short Sleeve Moisture	4	Generic
20	DANVOUY Womens T Shirt Casual Cotton Short	4	Generic
\.


--
-- Data for Name: fact_sales; Type: TABLE DATA; Schema: warehouse; Owner: -
--

COPY warehouse.fact_sales (sale_id, product_id, quantity, total_amount, sale_date) FROM stdin;
1	1	1	109.95	2025-10-23
2	2	1	22.30	2025-10-23
3	3	1	55.99	2025-10-23
4	4	1	15.99	2025-10-23
5	5	1	695.00	2025-10-23
6	6	1	168.00	2025-10-23
7	7	1	9.99	2025-10-23
8	8	1	10.99	2025-10-23
9	9	1	64.00	2025-10-23
10	10	1	109.00	2025-10-23
11	11	1	109.00	2025-10-23
12	12	1	114.00	2025-10-23
13	13	1	599.00	2025-10-23
14	14	1	999.99	2025-10-23
15	15	1	56.99	2025-10-23
16	16	1	29.95	2025-10-23
17	17	1	39.99	2025-10-23
18	18	1	9.85	2025-10-23
19	19	1	7.95	2025-10-23
20	20	1	12.99	2025-10-23
\.


--
-- Name: dim_category_category_id_seq; Type: SEQUENCE SET; Schema: warehouse; Owner: -
--

SELECT pg_catalog.setval('warehouse.dim_category_category_id_seq', 4, true);


--
-- Name: dim_date_date_id_seq; Type: SEQUENCE SET; Schema: warehouse; Owner: -
--

SELECT pg_catalog.setval('warehouse.dim_date_date_id_seq', 1, true);


--
-- Name: dim_product_product_id_seq; Type: SEQUENCE SET; Schema: warehouse; Owner: -
--

SELECT pg_catalog.setval('warehouse.dim_product_product_id_seq', 20, true);


--
-- Name: fact_sales_sale_id_seq; Type: SEQUENCE SET; Schema: warehouse; Owner: -
--

SELECT pg_catalog.setval('warehouse.fact_sales_sale_id_seq', 20, true);


--
-- Name: dim_category dim_category_category_name_key; Type: CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.dim_category
    ADD CONSTRAINT dim_category_category_name_key UNIQUE (category_name);


--
-- Name: dim_category dim_category_pkey; Type: CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.dim_category
    ADD CONSTRAINT dim_category_pkey PRIMARY KEY (category_id);


--
-- Name: dim_date dim_date_date_key; Type: CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.dim_date
    ADD CONSTRAINT dim_date_date_key UNIQUE (date);


--
-- Name: dim_date dim_date_pkey; Type: CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.dim_date
    ADD CONSTRAINT dim_date_pkey PRIMARY KEY (date_id);


--
-- Name: dim_product dim_product_pkey; Type: CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.dim_product
    ADD CONSTRAINT dim_product_pkey PRIMARY KEY (product_id);


--
-- Name: fact_sales fact_sales_pkey; Type: CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_sales
    ADD CONSTRAINT fact_sales_pkey PRIMARY KEY (sale_id);


--
-- Name: dim_product dim_product_category_id_fkey; Type: FK CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.dim_product
    ADD CONSTRAINT dim_product_category_id_fkey FOREIGN KEY (category_id) REFERENCES warehouse.dim_category(category_id);


--
-- Name: fact_sales fact_sales_product_id_fkey; Type: FK CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_sales
    ADD CONSTRAINT fact_sales_product_id_fkey FOREIGN KEY (product_id) REFERENCES warehouse.dim_product(product_id);


--
-- Name: fact_sales fact_sales_sale_date_fkey; Type: FK CONSTRAINT; Schema: warehouse; Owner: -
--

ALTER TABLE ONLY warehouse.fact_sales
    ADD CONSTRAINT fact_sales_sale_date_fkey FOREIGN KEY (sale_date) REFERENCES warehouse.dim_date(date);


--
-- PostgreSQL database dump complete
--

\unrestrict 5X5ChZKhRE7YNKAiI8LchscYAbGJBi0jCFGCm6nxowNIgzPMsxi94ff8u7ndm5r

