import pandas as pd
from pyVAMDC.radex.radex import Radex
from pyVAMDC.spectral.species import getSpeciesWithRestrictions

def main():
    df_target, _ = getSpeciesWithRestrictions(stoichiometric_formula="C2H")
    """df_target1, _ = getSpeciesWithRestrictions(stoichiometric_formula="CMgN")
    df_target = pd.concat([df_target, df_target1], ignore_index=True)"""

    df_collider, _ = getSpeciesWithRestrictions(stoichiometric_formula="H2")
    df_collider1, _ = getSpeciesWithRestrictions(stoichiometric_formula="He")
    df_collider = pd.concat([df_collider, df_collider1], ignore_index=True)

    print(f"\n--- Target species ({len(df_target)} rows) ---")
    print(df_target[["InChIKey", "stoichiometricFormula", "name", "ivoIdentifier"]])
    print(f"\n--- Collider species ({len(df_collider)} rows) ---")
    print(df_collider[["InChIKey", "stoichiometricFormula", "name"]])

    db_df_collision = pd.DataFrame({"ivoIdentifier": ["ivo://vamdc/basecol2015/vamdc-tap"]})
    db_df_spectro = pd.DataFrame({"ivoIdentifier": ["ivo://vamdc/cdms/vamdc-tap_12.07"]})

    radex = Radex()

    df_radex = radex.getRadex(
        target_df=df_target,
        collider_df=df_collider,
        db_df_collision=db_df_collision,
        db_df_spectro=db_df_spectro,
    )

    if not df_radex.empty:
        print("\n--- RADEX Results ---")
        pd.set_option("display.max_columns", None)
        pd.set_option("display.max_rows", None)
        pd.set_option("display.width", None)
        pd.set_option("display.max_colwidth", None)
        print(df_radex)
    else:
        print("\nNo RADEX entries found for the specified species.")

if __name__ == "__main__":
    main()
