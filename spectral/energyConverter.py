

def get_phisical_constants():
    physical_constants = {
        'c': 299792458 ,  # speed of light
        'h': 6.62607015e-34 , # Planck constant
        'K': 1.38064878066852E-23 ,# Boltzmann constnat
        'Ry': 10973731.56815712 # Rydberg constant
    }
    return physical_constants


def get_conversion_factors():
    physical_constants = get_phisical_constants()
    conversion_factors = {
        'energy': {
            'joule': 1.0,  # Base unit
            'millijoule': 0.001,
            'microjoule': 0.000001,
            'nanojoule': 0.000000001,
            'picojoule': 0.000000000001,
            'eV' :  1.602176634e-19,
            'erg' : 1e-7,
            'kelvin' : physical_constants['K'],
            'rydberg' : physical_constants['h']*physical_constants['c']*physical_constants['Ry'],
            'cm-1' : physical_constants['h']*physical_constants['c']*100
        },
        'frequency': {
            'hertz': 1.0,  # Base unit
            'kilohertz': 1000,
            'megahertz': 1e6,
            'gigahertz': 1e9,
            'terahertz': 1e12
        },
        'wavelength': {
            'meter': 1.0,  # Base unit
            'centimeter': 0.01,
            'millimeter': 0.001,
            'micrometer': 0.000001,
            'nanometer': 0.000000001,
            'angstrom': 0.0000000001
        }
    }
    return conversion_factors

def electromagnetic_convertersion(value, from_unit, to_unit):
    conversion_factors = get_conversion_factors()
    physical_constants = get_phisical_constants()
    

    if from_unit in conversion_factors['energy']:
        if to_unit in conversion_factors['energy']:
            # energy to energy 
            from_factor = conversion_factors['energy'][from_unit]
            to_factor = conversion_factors['energy'][to_unit]
            converted_value = value * from_factor/to_factor

        elif to_unit in conversion_factors['frequency']:
            # Energy to frequency
            from_factor = conversion_factors['energy'][from_unit]
            to_factor = conversion_factors['frequency'][to_unit]
            converted_value = from_factor * value /(to_factor *  physical_constants['h'])
        
        elif to_unit in conversion_factors['wavelength']:
            # Energy to wavelength
            from_factor = conversion_factors['energy'][from_unit]
            to_factor = conversion_factors['wavelength'][to_unit]
            converted_value = physical_constants['c']*physical_constants['h']/(value * from_factor * to_factor)
        else:
            raise ValueError(f"Invalid to_unit for energy: {to_unit}")
        
    elif from_unit in conversion_factors['frequency']:
        if to_unit in conversion_factors['frequency']:
            # Frequency to frequency
            from_factor = conversion_factors['frequency'][from_unit]
            to_factor = conversion_factors['frequency'][to_unit]
            converted_value = value * from_factor/to_factor
        
        elif to_unit in conversion_factors['energy']:
            # Frequency to energy
            from_factor = conversion_factors['frequency'][from_unit]
            to_factor = conversion_factors['energy'][to_unit]
            converted_value = value * from_factor * physical_constants['h']/to_factor
        
        elif to_unit in conversion_factors['wavelength']:
            # Frequency to wavelength
            from_factor = conversion_factors['frequency'][from_unit]
            to_factor = conversion_factors['wavelength'][to_unit]
            converted_value = physical_constants['c']/(value * from_factor * to_factor)
        else:
            raise ValueError(f"Invalid to_unit for frequency: {to_unit}")
        

    elif from_unit in conversion_factors['wavelength']:
        if to_unit in conversion_factors['wavelength']:
            # Wavelength to wavelenght
            from_factor = conversion_factors['wavelength'][from_unit]
            to_factor = conversion_factors['wavelength'][to_unit]
            converted_value = value * from_factor/to_factor

        elif to_unit in conversion_factors['energy']:
            # Wavelength to energy
            from_factor = conversion_factors['wavelength'][from_unit]
            to_factor = conversion_factors['energy'][to_unit]
            converted_value = physical_constants['c']*physical_constants['h']/(value*from_factor*to_factor)

        elif to_unit in conversion_factors['frequency']:
            # Wavelength to frequency
            from_factor = conversion_factors['wavelength'][from_unit]
            to_factor = conversion_factors['frequency'][to_unit]
            converted_value = physical_constants['c']/(value * from_factor * to_factor)
        else:
            raise ValueError(f"Invalid to_unit for wavelength: {to_unit}")
    
    else:
        raise ValueError(f"Invalid from_unit: {from_unit}")

    return converted_value
   

a = electromagnetic_convertersion(1 , 'eV', 'kelvin')
print(a)

