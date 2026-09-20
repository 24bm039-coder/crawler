"""
================================================================================
ALL-INDIA PIN CODE GOOGLE MAPS LEADS CRAWLER - SPLIT PART 303 / 400
================================================================================
- Group: ThirdTest_pincode_group_31_parts_301_to_310
- Assigned PIN Codes: 48 (Range: 688009 to 689111)
- Unique Categories: 256
- Total Search Combinations: 12,288 (Strict 12,288 scale!)
- Expected Run Duration: ~1 to 1.5 hours (Fast & Zero Timeout Risk)
- 4-Tier Output Folders (both CSV and JSON in all folders):
  1) master/                -> ALL_INDIA_LEADS_PART_303.csv & .json
  2) by_pincode/            -> <pincode>.csv & <pincode>.json
  3) by_category/           -> <category>.csv & <category>.json
  4) by_combination/        -> <pincode>_<category>.csv & .json
  5) pincode_city_reference/-> pincode_city_mapping_part_303.csv & .json
- Concurrency: 16 Workers (High-throughput & resilient)
================================================================================
"""

import os
import sys
import re
import csv
import time
import json
import random
import logging
import urllib.parse
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PART_ID = "part_303"
LEAD_AUTO_SAVE_THRESHOLD = 25000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Part-303] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(f"PincodeCrawler_{PART_ID}")

# Assigned PIN codes for this partition (48 PIN codes)
ASSIGNED_PINCODES = [
  "688009",
  "688011",
  "688012",
  "688013",
  "688014",
  "688501",
  "688502",
  "688503",
  "688504",
  "688505",
  "688506",
  "688521",
  "688522",
  "688523",
  "688524",
  "688525",
  "688526",
  "688527",
  "688528",
  "688529",
  "688530",
  "688531",
  "688532",
  "688533",
  "688534",
  "688535",
  "688536",
  "688537",
  "688538",
  "688539",
  "688540",
  "688541",
  "688555",
  "688561",
  "688562",
  "688570",
  "688582",
  "689101",
  "689102",
  "689103",
  "689104",
  "689105",
  "689106",
  "689107",
  "689108",
  "689109",
  "689110",
  "689111"
]

# 256 Unique Business Categories
CATEGORIES = [
  "Kirana Store",
  "Supermarket",
  "Departmental Store",
  "Provision Store",
  "Organic Food Store",
  "Dairy and Milk Parlour",
  "Fruit and Vegetable Wholesaler",
  "Dry Fruits and Spices Wholesaler",
  "Flour Mill",
  "Edible Oil Wholesaler",
  "Rice and Grain Merchant",
  "Meat and Poultry Shop",
  "Fish Market",
  "General Store",
  "Paan and FMCG Stall",
  "FMCG Distributor",
  "Frozen Food Distributor",
  "Pet Food and Pet Supplies",
  "Sweet Stall / Mithai Shop",
  "Bakery and Cake Shop",
  "Patisserie",
  "Tea Stall / Chai Cafe",
  "Juice Center and Milkshake Bar",
  "Pure Veg Restaurant",
  "Non-Veg Biryani Restaurant",
  "Dhaba and Highway Restaurant",
  "Tiffin Center and Mess",
  "South Indian Restaurant",
  "North Indian Restaurant",
  "Fast Food and Chaat Corner",
  "Cloud Kitchen",
  "Cafe and Coffee Shop",
  "Ice Cream Parlour",
  "Bar and Pub",
  "Family Restaurant",
  "Restaurant Chains",
  "Saree Showroom",
  "Silk Saree Wholesaler",
  "Readymade Garments Shop",
  "Mens Wear Showroom",
  "Womens Ethnic Wear and Kurti",
  "Kids Wear Store",
  "Tailor and Fashion Designer",
  "Textile Wholesaler and Fabric Merchant",
  "Gold and Diamond Jewellery Showroom",
  "Silver Jewellery Shop",
  "Goldsmith and Jewellery Repair",
  "Artificial Jewellery and Accessories",
  "Footwear and Shoe Store",
  "Leather Goods and Bags",
  "Handloom and Khadi Store",
  "Uniform Manufacturer",
  "Bridal Wear and Wedding Collection",
  "Hosiery and Undergarments Wholesaler",
  "Watch Showroom and Repair",
  "Optical Store and Eyewear",
  "Boutiques",
  "Luxury Clothing Shops",
  "Medical Store / Pharmacy",
  "24 Hour Pharmacy",
  "Ayurvedic Pharmacy and Clinic",
  "Homeopathic Clinic",
  "Multispeciality Hospital",
  "Nursing Home and Maternity Hospital",
  "Clinics",
  "Doctors",
  "Dental Clinic",
  "Eye Clinic and Eye Hospital",
  "Skin Clinic and Dermatologist",
  "Pediatrician and Child Clinic",
  "Orthopedic and Physiotherapy Clinic",
  "Diagnostic Center",
  "Pathology Lab and Blood Test",
  "Polyclinic",
  "Dialysis Center",
  "ENT Clinic",
  "Veterinary Clinic and Pet Hospital",
  "Surgical Equipment Supplier",
  "Medical Equipment Supplier",
  "Yoga Center",
  "Gym and Fitness Center",
  "Fitness Chains",
  "Healthcare Clinic Chains",
  "Two Wheeler Repair and Mechanic",
  "Car Repair Workshop and Garage",
  "Car Wash and Auto Detailing",
  "Two Wheeler Showroom and Dealer",
  "Car Showroom and Used Car Dealer",
  "Commercial Vehicle and Tractor Dealer",
  "Auto Spare Parts Shop",
  "Tyre Showroom and Puncture Shop",
  "Car and Bike Battery Dealer",
  "Auto Electrician and AC Repair",
  "CNG Kit Fitment Center",
  "Bicycle Shop and Repair",
  "Taxi Service and Car Rental",
  "Tour and Travel Operator",
  "Bus Booking Agency",
  "Packers and Movers",
  "Logistics and Transport Services",
  "Tempo and Mini Truck Service",
  "Crane and Towing Service",
  "Driving School",
  "Automotive Service Chains",
  "Hardware Store",
  "Electrical Goods and Lighting Store",
  "Sanitaryware and Bathroom Fittings",
  "Paint and Putty Dealer",
  "Tile and Marble Showroom",
  "Granite Dealer",
  "Plywood and Timber Merchant",
  "Glass and Mirror Merchant",
  "Cement and Sand Supplier",
  "TMT Steel and Iron Wholesaler",
  "Building Material Supplier",
  "Borewell Drilling Contractor",
  "Plumber",
  "Electrician",
  "AC Fridge and Washing Machine Repair",
  "RO Water Purifier Sales and Service",
  "Solar Rooftop and Inverter Dealer",
  "Interior Designers",
  "Architects",
  "Civil Contractor and Builder",
  "Roofing Sheet Supplier",
  "False Ceiling Contractor",
  "Waterproofing Contractor",
  "Modular Kitchen Manufacturer",
  "Furniture Showroom",
  "Salon",
  "Beauty Parlour",
  "Spa",
  "Unisex Salon",
  "Bridal Makeup Artist",
  "Cosmetics Wholesaler",
  "Tattoo and Nail Art Studio",
  "Herbal and Ayurvedic Cosmetic Products",
  "Hair Transplant Clinic",
  "Spa Equipment Suppliers",
  "Spa Consultants",
  "Wellness Center",
  "Therapy Center",
  "Marriage Hall / Kalyana Mandapam",
  "Banquet Hall",
  "Event Planners/Wedding Planners",
  "Flower Decorator",
  "Balloon Decorator",
  "Tent House and Shamiana",
  "Sound and Light Rental",
  "Caterer and Event Planner",
  "Photographers",
  "Videographer and Drone Rental",
  "Hotel",
  "Resort",
  "Hostels",
  "PG",
  "Guesthouse",
  "Trousseau Home Decor",
  "Gifting",
  "Cleaning and Hotel Supplier shops/ wholesalers",
  "Hotel Kit Suppliers",
  "Hospitality Consultants",
  "Media and Event",
  "Corporate Event Planner",
  "School",
  "Play School and Daycare",
  "Junior College and Degree College",
  "NEET and JEE Coaching Center",
  "Commerce and CA Coaching",
  "Spoken English Institute",
  "Computer Training Institute",
  "Competitive Exam Coaching (UPSC/Banking)",
  "Tuition Center",
  "Music and Dance Academy",
  "Sports Academy and Turf Ground",
  "Bookstore and Stationery Shop",
  "Educational Consultant",
  "Xerox and Photostat Center",
  "Printing Press and Offset Printer",
  "Flex and Banner Printing",
  "Wedding Invitation Card Printer",
  "Common Service Center (CSC) / E-Seva",
  "Internet Cafe",
  "Computer Sales and Laptop Repair",
  "CCTV Installation and Security System",
  "Mobile Phone Sales and Repair",
  "Mobile Accessories Wholesaler",
  "POS and Billing Software Vendor",
  "Document Writer and Stamp Vendor",
  "IT and Telecom Services",
  "Chartered Accountant (CA)",
  "Tax and GST Consultant",
  "Advocate and Lawyer",
  "Insurance Agent",
  "Home Loan DSA and Loan Consultant",
  "Money Transfer and Forex",
  "Microfinance and NBFC",
  "Pawn Broker and Gold Loan",
  "Chit Fund Company",
  "Stock Broker and Share Sub-broker",
  "Company Registration Consultant",
  "HR Planning and Recruitment",
  "Courier and Cargo Service",
  "Security Guard Agency",
  "Housekeeping Services",
  "Scrap Dealer and Raddi Wholesaler",
  "Financial and Legal Services",
  "Business and Audit Services",
  "Real Estate Agents",
  "Commercial Real Estate Brokerages",
  "Premium Luxury Real Estate",
  "Property Developers",
  "Steel Fabrication Workshop",
  "Welding and Lathe Works",
  "CNC Machining and Laser Cutting",
  "Aluminium Fabrication",
  "Plastic Molding Manufacturer",
  "Corrugated Box and Packaging Material Manufacturers",
  "Chemical Wholesalers",
  "Industrial Hardware and Fasteners",
  "Motor Rewinding and Pump Repair",
  "Generator Sales and Rental",
  "Warehouse and Cold Storage",
  "Rice Mill and Agro Processing",
  "Flour and Oil Mill",
  "Fertilizer and Pesticide Dealer",
  "Agricultural Machinery and Harvester",
  "Industrial Equipment Suppliers",
  "Importers",
  "Exporters",
  "EXIMS",
  "Tradeshows",
  "Exhibitions",
  "Digital Marketing Agencies",
  "Local SEO Agencies",
  "SEO Agencies",
  "SEO Consultants",
  "PPC Advertising Agencies",
  "Social Media Marketing Agencies",
  "Advertisement Agency",
  "Growth Marketing",
  "Lead Generation Agencies",
  "B2B Appointment-Setting Agencies",
  "Telemarketing Firms",
  "SaaS Companies Selling to SMBs",
  "CRM Data Enrichment Companies",
  "Market Research Firms",
  "Malls",
  "Shopping Mall Operators",
  "Multi-location Retail Chains",
  "Commercial Complex",
  "Wholesale Market / Mandi",
  "Industrial Estate / GIDC / MIDC / SIPCOT",
  "Shops",
  "Offices",
  "Businesses"
]

# Pincode to City/Region/Circle Metadata Map
PINCODE_METADATA = {
  "688009": {
    "pincode": "688009",
    "circle": "Kerala Circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Pazhaveedu SO",
      "Pandarakulam BO"
    ]
  },
  "688011": {
    "pincode": "688011",
    "circle": "Kerala circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Kuppappuram BO",
      "Alappuzha Iron Bridge SO",
      "Chungom BO",
      "Alappuzha District Hospital SO",
      "Alappuzha Mullackal SO"
    ]
  },
  "688012": {
    "pincode": "688012",
    "circle": "Kerala Circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Alappuzha Bazar SO"
    ]
  },
  "688013": {
    "pincode": "688013",
    "circle": "Kerala Circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Thathampally SO",
      "Thondankulangara SO"
    ]
  },
  "688014": {
    "pincode": "688014",
    "circle": "Kerala Circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Punnapra North SO"
    ]
  },
  "688501": {
    "pincode": "688501",
    "circle": "Kerala Circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Kainakary SO",
      "Chennamkary BO",
      "Kainakary East BO",
      "Kuttamangalam BO",
      "Thottuvathala BO"
    ]
  },
  "688502": {
    "pincode": "688502",
    "circle": "Kerala Circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Moncombu SO",
      "Chathurthiakary BO"
    ]
  },
  "688503": {
    "pincode": "688503",
    "circle": "Kerala Circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Moncombu Thekkekara SO",
      "Nedumudy BO",
      "Ponga BO"
    ]
  },
  "688504": {
    "pincode": "688504",
    "circle": "Kerala Circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Pulincunnu SO",
      "Kannady BO",
      "Punnakunnathussery BO",
      "Kannady West BO",
      "Kayalpuram BO",
      "Punnakunnathussery South BO"
    ]
  },
  "688505": {
    "pincode": "688505",
    "circle": "Kerala Circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Champakulam SO",
      "Chempumpuram BO",
      "Pullangadi BO",
      "Champakulam East BO"
    ]
  },
  "688506": {
    "pincode": "688506",
    "circle": "Kerala Circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Kavalam SO",
      "Chennamkary East BO",
      "Cherukara Kuttanad BO",
      "Kavalam North BO",
      "Narakathara BO"
    ]
  },
  "688521": {
    "pincode": "688521",
    "circle": "Kerala Circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Pathirappally SO"
    ]
  },
  "688522": {
    "pincode": "688522",
    "circle": "Kerala Circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Kalavoor SO",
      "Kattor Kalavoor BO",
      "Pollethai BO"
    ]
  },
  "688523": {
    "pincode": "688523",
    "circle": "Kerala Circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Sethulekshmipuram SO",
      "Mararikulam North BO"
    ]
  },
  "688524": {
    "pincode": "688524",
    "circle": "Kerala Circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Cherthala HO",
      "Cherthala Cutcherry SO",
      "Kalavamkodam B O"
    ]
  },
  "688525": {
    "pincode": "688525",
    "circle": "Kerala Circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Muhamma SO"
    ]
  },
  "688526": {
    "pincode": "688526",
    "circle": "Kerala Circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Poochackal SO",
      "Manappuram BO",
      "Naduvathunagar BO",
      "Olavaipu BO",
      "Panavally BO",
      "Trichattukulam BO"
    ]
  },
  "688527": {
    "pincode": "688527",
    "circle": "Kerala Circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Thanneermukkom SO",
      "Kannankara BO",
      "Karikkad BO",
      "Kokkothamangalam BO",
      "Muttathiparambu BO"
    ]
  },
  "688528": {
    "pincode": "688528",
    "circle": "Kerala Circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Thycattussery SO"
    ]
  },
  "688529": {
    "pincode": "688529",
    "circle": "Kerala Circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Kadakkarapally SO",
      "Vettackal BO"
    ]
  },
  "688530": {
    "pincode": "688530",
    "circle": "Kerala Circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Arthingal SO",
      "Chethy BO",
      "Thaickal BO"
    ]
  },
  "688531": {
    "pincode": "688531",
    "circle": "Kerala Circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Pattanacaud SO",
      "Andhakaranazhy BO"
    ]
  },
  "688532": {
    "pincode": "688532",
    "circle": "Kerala Circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Thuravoor SO",
      "Valamangalam BO",
      "Thuravoor South BO"
    ]
  },
  "688533": {
    "pincode": "688533",
    "circle": "Kerala Circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Kuthiathode SO"
    ]
  },
  "688534": {
    "pincode": "688534",
    "circle": "Kerala Circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Aroor SO"
    ]
  },
  "688535": {
    "pincode": "688535",
    "circle": "Kerala Circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Arookutty SO",
      "Vaduthala Jetty BO"
    ]
  },
  "688536": {
    "pincode": "688536",
    "circle": "Kerala Circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Vayalar SO"
    ]
  },
  "688537": {
    "pincode": "688537",
    "circle": "Kerala Circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Eramallur SO",
      "Chandiroor BO",
      "Ezhupunna BO",
      "Ezhupunna South BO"
    ]
  },
  "688538": {
    "pincode": "688538",
    "circle": "Kerala Circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Mannancherry SO",
      "Aryad North BO",
      "Ponnad BO"
    ]
  },
  "688539": {
    "pincode": "688539",
    "circle": "Kerala Circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Mayithara Market SO",
      "Cherthala South BO",
      "Kuruppankulangara BO",
      "Maruthorvattom BO",
      "Varanad BO"
    ]
  },
  "688540": {
    "pincode": "688540",
    "circle": "Kerala Circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Thirumalabhagom SO",
      "Pallithode BO",
      "Parayakad BO"
    ]
  },
  "688541": {
    "pincode": "688541",
    "circle": "Kerala Circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Pallippuram SO",
      "Kochuramapuram BO",
      "Thirunallur BO"
    ]
  },
  "688555": {
    "pincode": "688555",
    "circle": "Kerala Circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Varanam SO"
    ]
  },
  "688561": {
    "pincode": "688561",
    "circle": "Kerala Circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Ambalapuzha SO",
      "Ambalapuzha East  B O",
      "Illichira BO",
      "Karumady BO",
      "Purakad BO",
      "Thottappally BO"
    ]
  },
  "688562": {
    "pincode": "688562",
    "circle": "Kerala Circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Thakazhy SO",
      "Chirayakom BO",
      "Kelamangalam BO",
      "Kunnumma Thakazhy BO"
    ]
  },
  "688570": {
    "pincode": "688570",
    "circle": "Kerala Circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Perumbalam SO"
    ]
  },
  "688582": {
    "pincode": "688582",
    "circle": "Kerala Circle",
    "region": "Kochi Region",
    "division": "Alleppey Division",
    "offices": [
      "Sreenarayanapuram SO",
      "Kanichukulangara BO"
    ]
  },
  "689101": {
    "pincode": "689101",
    "circle": "Kerala Circle",
    "region": "Trivandrum HQ Region",
    "division": "Tiruvalla Division",
    "offices": [
      "Tiruvalla HO",
      "Tiruvalla Market Jn SO"
    ]
  },
  "689102": {
    "pincode": "689102",
    "circle": "Kerala Circle",
    "region": "Trivandrum HQ Region",
    "division": "Tiruvalla Division",
    "offices": [
      "Kavumbhagom SO",
      "Kallumkal BO",
      "Venpala BO"
    ]
  },
  "689103": {
    "pincode": "689103",
    "circle": "Kerala Circle",
    "region": "Trivandrum HQ Region",
    "division": "Tiruvalla Division",
    "offices": [
      "Kuttapuzha SO",
      "Chumathra BO"
    ]
  },
  "689104": {
    "pincode": "689104",
    "circle": "Kerala Circle",
    "region": "Trivandrum HQ Region",
    "division": "Tiruvalla Division",
    "offices": [
      "Valanjavattom SO",
      "Valanjavattom East BO"
    ]
  },
  "689105": {
    "pincode": "689105",
    "circle": "Kerala Circle",
    "region": "Trivandrum HQ Region",
    "division": "Tiruvalla Division",
    "offices": [
      "Manjady Jn SO",
      "Kattode BO",
      "Pushpagiri Tiruvalla BO"
    ]
  },
  "689106": {
    "pincode": "689106",
    "circle": "Kerala Circle",
    "region": "Trivandrum HQ Region",
    "division": "Tiruvalla Division",
    "offices": [
      "Kuttur SO Pathanamthitta",
      "Thengeli BO"
    ]
  },
  "689107": {
    "pincode": "689107",
    "circle": "Kerala Circle",
    "region": "Trivandrum HQ Region",
    "division": "Tiruvalla Division",
    "offices": [
      "Muthoor SO",
      "Perumthuruthy BO"
    ]
  },
  "689108": {
    "pincode": "689108",
    "circle": "Kerala Circle",
    "region": "Trivandrum HQ Region",
    "division": "Tiruvalla Division",
    "offices": [
      "Peringara SO",
      "Karakkal BO"
    ]
  },
  "689109": {
    "pincode": "689109",
    "circle": "Kerala Circle",
    "region": "Trivandrum HQ Region",
    "division": "Tiruvalla Division",
    "offices": [
      "Tiruvanvandur SO",
      "Eramallikkara BO"
    ]
  },
  "689110": {
    "pincode": "689110",
    "circle": "Kerala Circle",
    "region": "Trivandrum HQ Region",
    "division": "Tiruvalla Division",
    "offices": [
      "Podiyadi SO",
      "Nedumpuram BO"
    ]
  },
  "689111": {
    "pincode": "689111",
    "circle": "Kerala Circle",
    "region": "Trivandrum HQ Region",
    "division": "Tiruvalla Division",
    "offices": [
      "Tiruvalla RS SO"
    ]
  }
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
]

class SplitPincodeLeadCrawler:
    def __init__(self, max_workers=16):
        self.max_workers = max_workers
        self.session = self._create_resilient_session()
        self.results = []
        self.seen_keys = set()
        self.completed_combos = set()
        self.last_git_push_count = 0
        
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.part_dir = os.path.join(self.script_dir, PART_ID)
        
        # 4 Output Directories
        self.master_dir = os.path.join(self.part_dir, "master")
        self.by_pincode_dir = os.path.join(self.part_dir, "by_pincode")
        self.by_category_dir = os.path.join(self.part_dir, "by_category")
        self.combos_dir = os.path.join(self.part_dir, "by_combination")
        self.ref_dir = os.path.join(self.part_dir, "pincode_city_reference")
        
        for d in [self.master_dir, self.by_pincode_dir, self.by_category_dir, self.combos_dir, self.ref_dir]:
            os.makedirs(d, exist_ok=True)
            
        self.checkpoint_file = os.path.join(self.part_dir, f"checkpoint_{PART_ID}.json")
        self.save_reference_metadata()
        self.load_checkpoint()

    def save_reference_metadata(self):
        try:
            ref_json = os.path.join(self.ref_dir, f"pincode_city_mapping_{PART_ID}.json")
            ref_csv = os.path.join(self.ref_dir, f"pincode_city_mapping_{PART_ID}.csv")
            with open(ref_json, 'w', encoding='utf-8') as f:
                json.dump(PINCODE_METADATA, f, indent=2, ensure_ascii=False)
            with open(ref_csv, 'w', newline='', encoding='utf-8-sig') as f:
                w = csv.DictWriter(f, fieldnames=["pincode", "circle", "region", "division", "offices"])
                w.writeheader()
                for p, meta in PINCODE_METADATA.items():
                    w.writerow({
                        "pincode": meta.get("pincode", p),
                        "circle": meta.get("circle", "N/A"),
                        "region": meta.get("region", "N/A"),
                        "division": meta.get("division", "N/A"),
                        "offices": ", ".join(meta.get("offices", []))
                    })
        except Exception as e:
            logger.warning(f"Could not save reference metadata: {e}")

    def _create_resilient_session(self):
        s = requests.Session()
        retries = Retry(total=5, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries, pool_connections=64, pool_maxsize=64)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        s.headers.update({
            "User-Agent": random.choice(USER_AGENTS),
            "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
            "Accept": "*/*",
            "Referer": "https://www.google.com/"
        })
        return s

    def load_checkpoint(self):
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.completed_combos = set(data.get("completed_combos", []))
                    logger.info(f"Loaded checkpoint: {len(self.completed_combos)} combinations already completed.")
            except Exception as e:
                logger.warning(f"Failed to load checkpoint: {e}")

    def save_checkpoint(self):
        try:
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump({"completed_combos": list(self.completed_combos), "updated_at": datetime.now().isoformat()}, f)
        except Exception as e:
            logger.warning(f"Failed to save checkpoint: {e}")

    def _extract_phone(self, details):
        def deep_search(obj):
            if isinstance(obj, str):
                cleaned = obj.strip()
                if re.match(r"^(\+91[\-\s]?)?[0]?(91)?[6789]\d{9}$", cleaned) or (cleaned.startswith("+91") and len(cleaned) >= 13):
                    return cleaned
                if re.match(r"^0\d{2,4}[\-\s]?\d{6,8}$", cleaned):
                    return cleaned
            elif isinstance(obj, list):
                for item in obj:
                    res = deep_search(item)
                    if res:
                        return res
            elif isinstance(obj, dict):
                for v in obj.values():
                    res = deep_search(v)
                    if res:
                        return res
            return None
        found = deep_search(details)
        return found if found else "N/A"

    def _generate_search_angles(self, pincode, category):
        return [
            f"{category} in {pincode}",
            f"Best {category} in {pincode}",
            f"{category} near {pincode}",
            f"{category} dealers suppliers in {pincode}"
        ]

    def git_auto_push_milestone(self, lead_count):
        logger.info("=" * 60)
        logger.info(f"[*] AUTO-SAVE TRIGGERED: {lead_count:,} Leads Scraped! Committing to GitHub...")
        logger.info("=" * 60)
        
        self.export_all()
        self.save_checkpoint()
        
        try:
            repo_root = os.path.abspath(os.path.join(self.script_dir, ".."))
            subprocess.run(["git", "config", "user.name", "github-actions[bot]"], cwd=repo_root, capture_output=True)
            subprocess.run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"], cwd=repo_root, capture_output=True)
            
            rel_part = os.path.relpath(self.part_dir, repo_root)
            subprocess.run(["git", "add", "-A", rel_part], cwd=repo_root, capture_output=True)
            commit_msg = f"Auto-save milestone: {lead_count:,} leads scraped for {PART_ID}"
            subprocess.run(["git", "commit", "-m", commit_msg], cwd=repo_root, capture_output=True)
            
            subprocess.run(["git", "pull", "--rebase", "origin", "main"], cwd=repo_root, capture_output=True)
            push_res = subprocess.run(["git", "push", "origin", "HEAD:main"], cwd=repo_root, capture_output=True, text=True)
            
            if push_res.returncode == 0:
                logger.info(f"[+] SUCCESS: Auto-saved {lead_count:,} leads directly to GitHub repository!")
            else:
                logger.warning(f"[!] Git push notice: {push_res.stderr.strip()}")
        except Exception as git_err:
            logger.warning(f"[!] Git auto-push exception: {git_err}")

    def scrape_single_pair(self, pincode, category):
        combo_key = f"{pincode}_{category}"
        if combo_key in self.completed_combos:
            return []

        leads_for_combo = []
        local_seen = set()
        search_angles = self._generate_search_angles(pincode, category)
        meta = PINCODE_METADATA.get(pincode, {})

        for q in search_angles:
            encoded_q = urllib.parse.quote(q)
            pb_str = (
                f"!1s{encoded_q}!7i20!10b1!12m59!1m5!18b1!30b1!31m1!1b1!34e1!2m4!5m1!6e2!20e3!39b1"
                f"!6m31!32i1!49b1!63m0!66b1!85b1!114b1!149b1!206b1!209b1!212b1!215b1!216b1!222b1!223b1!232b1!234b1!235b1"
                f"!246b1!253b1!260b1!262b1!266b1!270b1!271b1!273b1!280b1!281b1!291m0!294b1!302i300!303i100!10b1!12b1!13b1"
                f"!14b1!16b1!17m1!3e1!20m4!5e2!6b1!8b1!14b1!46m1!1b0!96b1!99b1!19m4!2m3!1i360!2i120!4i8!20m57!2m2!1i0"
                f"!2i20!3m2!2i4!5b1!6m6!1m2!1i86!2i86!1m2!1i408!2i240!7m33!1m3!1e1!2b0!3e3!1m3!1e2!2b1!3e2!1m3!1e2!2b0"
                f"!3e3!1m3!1e8!2b0!3e3!1m3!1e10!2b0!3e3!1m3!1e10!2b1!3e2!1m3!1e10!2b0!3e4!1m3!1e9!2b1!3e2!2b1!9b0!15m8"
                f"!1m7!1m2!1m1!1e2!2m2!1i195!2i195!3i20"
            )
            url = f"https://www.google.com/search?tbm=map&authuser=0&hl=en&gl=in&q={encoded_q}&pb={pb_str}"

            try:
                resp = self.session.get(url, timeout=(3.0, 7.0))
                time.sleep(0.10)

                if resp.status_code == 200:
                    raw_text = resp.text
                    if raw_text.startswith(")]}'"):
                        raw_text = raw_text[raw_text.find('['):]

                    data = json.loads(raw_text)
                    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], list) and len(data[0]) > 1:
                        places_raw = data[0][1]
                        if isinstance(places_raw, list):
                            for p in places_raw:
                                if not isinstance(p, list) or len(p) < 15:
                                    continue
                                d = p[14]
                                if not isinstance(d, list) or len(d) <= 11:
                                    continue

                                name = d[11] if len(d) > 11 and isinstance(d[11], str) else None
                                if not name:
                                    continue

                                place_id = d[78] if len(d) > 78 and d[78] else (d[0] if len(d) > 0 else "N/A")
                                dedup_key = place_id if place_id != "N/A" else f"{name}_{pincode}".lower()

                                if dedup_key in self.seen_keys or dedup_key in local_seen:
                                    continue
                                local_seen.add(dedup_key)
                                self.seen_keys.add(dedup_key)

                                categories_list = d[13] if len(d) > 13 and isinstance(d[13], list) else []
                                primary_category = categories_list[0] if categories_list else category
                                all_categories_str = ", ".join(categories_list) if categories_list else primary_category

                                rating = d[4][7] if len(d) > 4 and isinstance(d[4], list) and len(d[4]) > 7 else None
                                reviews_count = d[4][8] if len(d) > 4 and isinstance(d[4], list) and len(d[4]) > 8 else None

                                website = "N/A"
                                if len(d) > 7 and isinstance(d[7], list) and len(d[7]) > 0 and d[7][0]:
                                    website = str(d[7][0])

                                lat = d[9][2] if len(d) > 9 and isinstance(d[9], list) and len(d[9]) > 2 else None
                                lng = d[9][3] if len(d) > 9 and isinstance(d[9], list) and len(d[9]) > 3 else None

                                address = d[39] if len(d) > 39 and d[39] else (d[18] if len(d) > 18 and d[18] else f"{name}, {pincode}, India")
                                area = d[14] if len(d) > 14 and d[14] else str(pincode)

                                phone = self._extract_phone(d)
                                place_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}" if place_id != "N/A" else "N/A"

                                record = {
                                    "business_name": name,
                                    "search_category": category,
                                    "primary_category": primary_category,
                                    "all_categories": all_categories_str,
                                    "pincode": pincode,
                                    "circle": meta.get("circle", "N/A"),
                                    "region": meta.get("region", "N/A"),
                                    "division": meta.get("division", "N/A"),
                                    "major_offices": ", ".join(meta.get("offices", [])[:3]),
                                    "phone_number": phone,
                                    "website": website,
                                    "rating": rating,
                                    "reviews_count": reviews_count,
                                    "address": address,
                                    "area": area,
                                    "latitude": lat,
                                    "longitude": lng,
                                    "place_id": place_id,
                                    "place_url": place_url,
                                    "part_id": PART_ID,
                                    "crawled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                }
                                leads_for_combo.append(record)
                elif resp.status_code == 429:
                    logger.warning(f"Rate limited on ({pincode}, {category}). Backing off 3s...")
                    time.sleep(3.0)
            except Exception as err:
                logger.debug(f"Notice for ({pincode}, {category}): {err}")

        if leads_for_combo:
            safe_cat = re.sub(r'[^a-zA-Z0-9_]', '_', category).strip('_').lower()
            out_json = os.path.join(self.combos_dir, f"{pincode}_{safe_cat}.json")
            out_csv = os.path.join(self.combos_dir, f"{pincode}_{safe_cat}.csv")
            try:
                with open(out_json, 'w', encoding='utf-8') as f:
                    json.dump(leads_for_combo, f, indent=2, ensure_ascii=False)
                df_c = pd.DataFrame(leads_for_combo)
                df_c.to_csv(out_csv, index=False, encoding='utf-8-sig')
            except Exception as e:
                logger.warning(f"Failed to write combo files: {e}")

        self.completed_combos.add(combo_key)
        return leads_for_combo

    def crawl_all(self):
        all_combinations = [(p, c) for p in ASSIGNED_PINCODES for c in CATEGORIES]
        remaining = [(p, c) for (p, c) in all_combinations if f"{p}_{c}" not in self.completed_combos]
        total_tasks = len(all_combinations)

        logger.info("=" * 60)
        logger.info(f"STARTING CRAWLER PART          : {PART_ID}")
        logger.info(f"Assigned PIN Codes             : {len(ASSIGNED_PINCODES):,}")
        logger.info(f"Target Categories              : {len(CATEGORIES):,}")
        logger.info(f"Total Combinations (Tasks)     : {total_tasks:,}")
        logger.info(f"Remaining Combinations         : {len(remaining):,}")
        logger.info(f"Workers / Concurrency          : {self.max_workers} Threads")
        logger.info("=" * 60)

        completed_count = total_tasks - len(remaining)
        chunk_size = 500

        for chunk_idx in range(0, len(remaining), chunk_size):
            chunk = remaining[chunk_idx:chunk_idx + chunk_size]
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_map = {executor.submit(self.scrape_single_pair, pin, cat): (pin, cat) for pin, cat in chunk}
                for future in as_completed(future_map):
                    pin, cat = future_map[future]
                    completed_count += 1
                    try:
                        records = future.result()
                        if records:
                            self.results.extend(records)
                            logger.info(f"[{completed_count}/{total_tasks}] ({pin} | {cat}) -> Extracted {len(records)} leads | Total: {len(self.results):,} leads")
                            
                            if len(self.results) - self.last_git_push_count >= LEAD_AUTO_SAVE_THRESHOLD:
                                self.last_git_push_count = len(self.results)
                                self.git_auto_push_milestone(len(self.results))
                    except Exception as e:
                        logger.error(f"Error crawling ({pin}, {cat}): {e}")

            self.save_checkpoint()
            if len(self.results) - self.last_git_push_count >= LEAD_AUTO_SAVE_THRESHOLD:
                self.last_git_push_count = len(self.results)
                self.git_auto_push_milestone(len(self.results))

        self.export_all()
        self.git_auto_push_milestone(len(self.results))
        return len(self.results)

    def export_all(self):
        if not self.results:
            logger.warning("No results to export.")
            return

        for idx, item in enumerate(self.results):
            item["s_no"] = idx + 1

        fields = [
            "s_no", "business_name", "search_category", "primary_category", "all_categories",
            "pincode", "circle", "region", "division", "major_offices",
            "phone_number", "website", "rating", "reviews_count",
            "address", "area", "latitude", "longitude", "place_id", "place_url",
            "part_id", "crawled_at"
        ]

        # 1. Master Output (CSV and JSON)
        master_csv = os.path.join(self.master_dir, f"ALL_INDIA_LEADS_{PART_ID.upper()}.csv")
        master_json = os.path.join(self.master_dir, f"ALL_INDIA_LEADS_{PART_ID.upper()}.json")
        df_master = pd.DataFrame(self.results)
        df_master.to_csv(master_csv, index=False, encoding='utf-8-sig')
        with open(master_json, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False)
        logger.info(f"[+] Exported Master: {len(self.results):,} leads to CSV and JSON")

        # 2. By Pincode Output (CSV and JSON)
        by_pin = {}
        for r in self.results:
            by_pin.setdefault(str(r.get("pincode")), []).append(r)
        for pin, pin_leads in by_pin.items():
            if not pin: continue
            df_p = pd.DataFrame(pin_leads)
            df_p.to_csv(os.path.join(self.by_pincode_dir, f"{pin}.csv"), index=False, encoding='utf-8-sig')
            with open(os.path.join(self.by_pincode_dir, f"{pin}.json"), 'w', encoding='utf-8') as f:
                json.dump(pin_leads, f, indent=2, ensure_ascii=False)
        logger.info(f"[+] Exported by_pincode: {len(by_pin)} pincode files (both .csv & .json)")

        # 3. By Category Output (CSV and JSON)
        by_cat = {}
        for r in self.results:
            by_cat.setdefault(str(r.get("search_category")), []).append(r)
        for cat, cat_leads in by_cat.items():
            safe_cat = re.sub(r'[^a-zA-Z0-9_]', '_', cat).strip('_').lower()
            df_c = pd.DataFrame(cat_leads)
            df_c.to_csv(os.path.join(self.by_category_dir, f"{safe_cat}.csv"), index=False, encoding='utf-8-sig')
            with open(os.path.join(self.by_category_dir, f"{safe_cat}.json"), 'w', encoding='utf-8') as f:
                json.dump(cat_leads, f, indent=2, ensure_ascii=False)
        logger.info(f"[+] Exported by_category: {len(by_cat)} category files (both .csv & .json)")

def main():
    crawler = SplitPincodeLeadCrawler(max_workers=16)
    crawler.crawl_all()

if __name__ == "__main__":
    main()
