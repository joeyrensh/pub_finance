# from utility.emcookie_generation import CookieGeneration

# CookieGeneration().generate_em_cookies()

from utility.get_proxy import ProxyManager

proxy = ProxyManager().get_working_proxy()
print(proxy)
