{% macro map_country_to_region(column) %}
case 
    when {{ column }} in 
        (
        
        'AI', 'BM', 'CUK', 'FK', 'GB', 'GI', 'IO', 'MS', 'SH', 'TC', 'UK',
        
        'Anguilla', 'Bermuda', 'United Kingdom', 'Falkland Islands (Islas Malvinas)', 'Great Britain', 'Gibraltar', 'British Indian Ocean Territory', 
        'Montserrat', 'St. Helena', 'Turks & Caicos Islands', 'Turks and Caicos Islands', 'United Kingdom'

        )         
        
                                                then 'UK'
    
    when {{ column }} in 
        ('CA', 'Canada')                        then 'CA'
    
    when {{ column }} in 
        ('AU', 'AUS', 'Australia')              then 'AU'

    when {{ column }} in 
		    (
                'AS', 'CUS', 'GU', 'MP', 'PR', 'US', 'VI',
                
                'American Samoa', 'United States', 'Guam', 'Northern Mariana Islands', 'Puerto Rico', 'United States', 'U.S. Virgin Islands'
            )           
                                                
                                                then 'US'
    
    when {{ column }} in 
        (
            'AD', 'AL', 'AM', 'AT', 'AX', 'BA', 'BE', 'BG', 'BY', 'CH', 'CY', 'CZ', 'DE', 'DK', 'EE', 'ES', 'EU', 'FI', 
            'FO', 'FR', 'GE', 'GL', 'GR', 'HR', 'HU', 'IE', 'IS', 'IT', 'LI', 'LT', 'LU', 'LV', 'MC', 'MD', 'ME', 'MK', 
            'MT', 'NL', 'NO', 'PL', 'PT', 'RO', 'RS', 'RU', 'SE', 'SI', 'SJ', 'SK', 'SM', 'TR', 'UA', 'XK', 'PM', 'Europe',

            'Andorra', 'Albania', 'Armenia', 'Austria', 'Åland Islands', 'Bosnia & Herzegovina', 'Belgium', 'Bulgaria', 'Belarus', 'Switzerland', 'Cyprus', 
            'Czech Republic', 'Czechia', 'Germany', 'Denmark', 'Estonia', 'Spain', 'European Union', 'Finland', 'Faroe Islands', 'France', 'Georgia', 'Greenland', 
            'Greece', 'Croatia', 'Hungary', 'Ireland', 'Iceland', 'Italy', 'Liechtenstein', 'Lithuania', 'Luxembourg', 'Latvia', 'Monaco', 'Moldova', 'Montenegro', 
            'North Macedonia', 'Malta', 'Netherlands', 'Norway', 'Poland', 'Portugal', 'Romania', 'Serbia', 'Russia', 'Sweden', 'Slovenia', 'Svalbard and Jan Mayen', 
            'Svalbard & Jan Mayen', 'Slovakia', 'San Marino', 'Turkey', 'Türkiye', 'Ukraine', 'Kosovo', 'St. Pierre & Miquelon'
        )
                                            
                                                then 'EU'

                                                else 'ROW'

end
{% endmacro %}