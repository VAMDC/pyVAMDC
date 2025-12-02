from pyVAMDC.radex.radex import Radex
from pyVAMDC.spectral.species import getSpeciesWithRestrictions

def main():
    # Search species with stoichiometric formula
    df_species_target, _ = getSpeciesWithRestrictions(stoichiometric_formula="AlCN")
    df_species_collider, _ = getSpeciesWithRestrictions(stoichiometric_formula="H2")

    print("Target species:")
    print(df_species_target[['InChIKey', 'stoichiometricFormula', 'name']])
    print("\nCollider species:")
    print(df_species_collider[['InChIKey', 'stoichiometricFormula', 'name']])

    # Initialize Radex API client (default: http://127.0.0.1:8000)
    radex = Radex()

    # Search RADEX data via API for target-collider combinations
    df_radex = radex.getRadexCrossProduct(df_species_target, df_species_collider)

    if not df_radex.empty:
        print("\nRADEX Results:")
        print(df_radex.columns)
        print("---------------\n")
        print(df_radex[['idRadex', 'fileName', 'specieTarget', 'specieCollider',
                        'symmetryTarget', 'symmetryCollider']])

        # Display file URLs for the first entry
        if len(df_radex) > 0:
            urls = radex.displayFileUrls(df_radex, index=0)
            print(f"\nReturned URLs: {urls}")

    else:
        print("\nNo RADEX entries found for the specified species.")

if __name__ == "__main__":
    main()   