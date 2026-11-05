-- DEVELOPMENT SEED ONLY: the 35 agents of the fleet simulator (agent 'agent-<service>', key 'dev-agent-<service>').
-- Generated from the former src/main/resources/dev-agents.yml. The keys are public dev values: a production deployment
-- must leave this location out (GATEWAY_DB_LOCATIONS=classpath:db/migration, see application.yml).
-- Repeatable migration: re-run when this file changes; ON CONFLICT DO NOTHING never overwrites an agent (e.g. one that
-- was revoked on purpose). Each api_key_hash is sha256('dev-agent-<service>').
INSERT INTO agent (agent_id, display_name, api_key_hash, allowed_services) VALUES
    ('agent-checkout-api', 'Simulator agent for checkout-api', 'c9776052fb5baf052af7a6c2a079a77b2be8bf679de0e07c83fbebe92bff8ecb', ARRAY['checkout-api']),
    ('agent-cart-api', 'Simulator agent for cart-api', '674a3743df3f1b32c1566d9651832fd9d7f9825f0596e9b6541d72b3b2a9b04a', ARRAY['cart-api']),
    ('agent-catalog-api', 'Simulator agent for catalog-api', '3fc63825fe84200efbd99962e5481c654e67dbdf76c39381e5ecc6c7384ebe09', ARRAY['catalog-api']),
    ('agent-search-api', 'Simulator agent for search-api', 'b82f3bedbc5c9ab6623d7ee434bdd80c4ce303e78404b9d59eb7bd035b7e13b4', ARRAY['search-api']),
    ('agent-pricing-api', 'Simulator agent for pricing-api', '5329d762c49cf90831cba9f929482be15ca8091ff351a7c2ee3aa2e4a39f3467', ARRAY['pricing-api']),
    ('agent-inventory-api', 'Simulator agent for inventory-api', '555db6446dbc9bd1f42141b1247d8eccd85a247aed25bc85794e4099d45b3a47', ARRAY['inventory-api']),
    ('agent-order-api', 'Simulator agent for order-api', '3774eb550078cb6d3eee5c6e0b6c100c11ac75d2a765e44b5a695ea3996c7db0', ARRAY['order-api']),
    ('agent-payment-api', 'Simulator agent for payment-api', '12ae0317a42d9bde73e1ae230692684881e404fd11e648adde75c6ce107480db', ARRAY['payment-api']),
    ('agent-shipping-api', 'Simulator agent for shipping-api', 'dff9ac15d678db88839a4ef896ae3c357f2d1f19f21db768378d30627f83f2f6', ARRAY['shipping-api']),
    ('agent-notification-api', 'Simulator agent for notification-api', 'aef00331cd2bc2ceaee42a01a1ca165325bc1a434b4a24c5de1081958275b123', ARRAY['notification-api']),
    ('agent-user-api', 'Simulator agent for user-api', 'cd494607dfd086ab3fc61b1f283c2459920998a2416e9fd63e76e116493cd155', ARRAY['user-api']),
    ('agent-auth-api', 'Simulator agent for auth-api', '7fa271d2ec60fade7d13452b83b5cc4048a2b42272614bd99690a332f4800a2c', ARRAY['auth-api']),
    ('agent-profile-api', 'Simulator agent for profile-api', 'd7b05f0f4907124d893e1386132d3a2384efda6707cf671e4cebebac21020612', ARRAY['profile-api']),
    ('agent-review-api', 'Simulator agent for review-api', '0d64a2d9f5999ed4ca353233e205b567ed9f63c5f98a1683a7f1426b2bb6d880', ARRAY['review-api']),
    ('agent-recommendation-api', 'Simulator agent for recommendation-api', '26ad09bc7cebca141f0f97d3699284b1618b4e427f90fea535ef2200e41699ba', ARRAY['recommendation-api']),
    ('agent-media-api', 'Simulator agent for media-api', '7ffba510bc6408d2b1c6d727f9fb8dbb36532037658d19c413fa7f45bd128706', ARRAY['media-api']),
    ('agent-upload-api', 'Simulator agent for upload-api', 'ca1bd3d9fac6e9f8eb2dfa93be4ba4b5327b18737ba9d5d9f9fc6e12664b390f', ARRAY['upload-api']),
    ('agent-email-worker', 'Simulator agent for email-worker', 'dd2395d32c878fe47f3805f73ff74d524836cfc16ce8f07dade59440600ba52b', ARRAY['email-worker']),
    ('agent-sms-worker', 'Simulator agent for sms-worker', '890459f2e6b0865632dcafd16aee151c107d570b559b585eafc6e4bc6f976c26', ARRAY['sms-worker']),
    ('agent-report-worker', 'Simulator agent for report-worker', '6566ab7f5a7665655ade035b5dc5db28b998fd0954f64fdb97efdb99b01ce9da', ARRAY['report-worker']),
    ('agent-billing-api', 'Simulator agent for billing-api', 'ae8dea8d325c15f15cf4e3b12d5a5a0c9bcae00df56eddf358f7bb2cac47ddb2', ARRAY['billing-api']),
    ('agent-invoice-api', 'Simulator agent for invoice-api', '08333cd55ddb63fc0707fda4e709de4a452019b7f61fb4e11779f840aa22cef5', ARRAY['invoice-api']),
    ('agent-tax-api', 'Simulator agent for tax-api', '818bc6a3cb7df6a74b3d6d2d6dd53b5f686219465c2ba2989f5480743d886de0', ARRAY['tax-api']),
    ('agent-fraud-api', 'Simulator agent for fraud-api', 'c36663ddaf27132a15fbf77bf00459ac599295292710ceb2fd875070f7387e5e', ARRAY['fraud-api']),
    ('agent-loyalty-api', 'Simulator agent for loyalty-api', '6a95be39a6999f43c33a8a9f56c31571c02bc5c623a3c63291392ba3ed51bcbf', ARRAY['loyalty-api']),
    ('agent-coupon-api', 'Simulator agent for coupon-api', 'bb075bc0bdaaa4d46aa145ab22cab8787552edee2d3c7c8bb338761854649132', ARRAY['coupon-api']),
    ('agent-wishlist-api', 'Simulator agent for wishlist-api', '1d401bcfe1ccf2d37cf566930ba423e6ba19596f2616072e6140a083eaa4dc8b', ARRAY['wishlist-api']),
    ('agent-analytics-ingest', 'Simulator agent for analytics-ingest', '3d721f01f3efef0fe80527609c913c18c0d72f87944bc880854ce7a54ee3a025', ARRAY['analytics-ingest']),
    ('agent-audit-api', 'Simulator agent for audit-api', '760fb20f24002fbdce1638cc94044b8f53b7e7a9cb2c5df4456a2066517503d7', ARRAY['audit-api']),
    ('agent-config-api', 'Simulator agent for config-api', 'eb123dd8989f75b158c2a134514861ca98ae2e1cf7cf20b705a2fd9dd416496d', ARRAY['config-api']),
    ('agent-gateway-edge', 'Simulator agent for gateway-edge', '897c362042aab9c664c7c546364a23cdca5be137a346a5b472782a3d71372db6', ARRAY['gateway-edge']),
    ('agent-session-api', 'Simulator agent for session-api', '6f7254d242399cd1b4b95b9ff8049a8a80c243fa675279fed499b2a39f0a38a5', ARRAY['session-api']),
    ('agent-geo-api', 'Simulator agent for geo-api', 'd1711f2a8bd2d8fcd27d4c955fc8a61aba5ea02e3fd5a17e7ba3f4c7eb3c11d6', ARRAY['geo-api']),
    ('agent-support-api', 'Simulator agent for support-api', '7a11473add847446df179a5ef86e983acdde34b6911bd2d85e5fcd3a010a20a3', ARRAY['support-api']),
    ('agent-returns-api', 'Simulator agent for returns-api', 'b7c4c1a2a4bd1969c027d9c46bbfce0eb061c43cb4f3d38b20571e86416b6c78', ARRAY['returns-api'])
ON CONFLICT (agent_id) DO NOTHING;
