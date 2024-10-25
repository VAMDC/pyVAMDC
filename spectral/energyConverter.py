def get_phisical_constants():
    """
    Defines values for fundamental physical constants
    """
    physical_constants = {
        'c': 299792458 ,  # speed of light
        'h': 6.62607015e-34 , # Planck constant
        'K': 1.38064878066852E-23 ,# Boltzmann constnat
        'Ry': 10973731.56815712 # Rydberg constant
    }
    return physical_constants


def get_conversion_factors():
    """
    Defines conversion factors between units.
    Base unit for energt is Joule.
    Base unit for frequency is Hertz.
    Base unit for wavelenght is meter.
    """
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
            'angstrom': 1.0e-10
        }
    }
    return conversion_factors

def electromagnetic_conversion(value, from_unit, to_unit):
    """
    Perform unit conversion between all the units generally used to represent energy for electromagnetic phenomena. Those include 
    energy, frequencies and wavelenghts. 

    Args:
        value : float
            The value (expressed in the unit defined by the variable 'from_unit') to convert.
        
        from_unit :str
            The unit one want to convert value from. Must take one of the following values: 
            joule, millijoule, nanojoule, picojoule, eV, erg, kelvin, rydberg, cm-1, hertz, kilohertz, megahertz
            gigahertz, terahertz, meter, centimeter,millimeter, micrometer, nanometer, angstrom

        to_unit :str
            The unit one want to convert value to. Must take one of the following values: 
            joule, millijoule, nanojoule, picojoule, eV, erg, kelvin, rydberg, cm-1, hertz, kilohertz, megahertz
            gigahertz, terahertz, meter, centimeter,millimeter, micrometer, nanometer, angstrom
        
    Returns:
        converted_value : float
            The valeu converted to the unit expressed by the content of the 'to_unit' variable.  
    """
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
   

class WrappingClass:
    """
    This class is for internal use and not to be used by end-users. 
    This is a wrapping, kind of hack, to call the function electromagnetic_conversion as it was a lambda function containing a single parameter. 
    This wrapping is necessary to easily perform unit conversion in dataframe columns and is used by the convert_dataframe_units function (this last is the end-user function users should use)
    """
    def __init__(self, from_unit, to_unit , conversion_function):
        self.from_unit = from_unit
        self.to_unit = to_unit
        self.conversion_function = conversion_function

    def wrapped_function(self, value_to_convert):
        return self.conversion_function(value_to_convert, self.from_unit, self.to_unit)


def convert_dataframe_units(input_df, input_col_name, input_col_unit, output_col_name, output_col_unit, delete_input_col=False):
    """
    Perform unit conversion for values in a specific column of a Pandas Dataframe. The conversion is between all the units generally used to represent energy for electromagnetic phenomena. Those include 
    energy, frequencies and wavelenghts. 

    Args:
        input_df: dataframe
            The dataframe containing the column one want to convert unit on.

        input_col_name: str
            The name of the dataframe's column one want to convert unit on.
        
        input_col_unit:str
            The unit one want to convert value from. Must take one of the following values: 
            joule, millijoule, nanojoule, picojoule, eV, erg, kelvin, rydberg, cm-1, hertz, kilohertz, megahertz
            gigahertz, terahertz, meter, centimeter,millimeter, micrometer, nanometer, angstrom

        output_col_name: str
            The name of the dataframe's column where converted values will be stored.

        output_col_unit: str
            The unit one want to convert value to. Must take one of the following values: 
            joule, millijoule, nanojoule, picojoule, eV, erg, kelvin, rydberg, cm-1, hertz, kilohertz, megahertz
            gigahertz, terahertz, meter, centimeter,millimeter, micrometer, nanometer, angstrom        

        delete_input_col: boolean
            If this flaf is true, the column containing input value is deleted. Default False.
        
    Returns:
        input_df : dataframe
            The dataframe containing a new column with the converted values.
    """
    wrappedConversion = WrappingClass(input_col_unit, output_col_unit, electromagnetic_conversion)
    input_df[output_col_name] = input_df[input_col_name].apply(lambda x : wrappedConversion.wrapped_function(x))

    if delete_input_col is True:
        input_df = input_df.drop(input_col_name, axis = 1)

    return input_df

