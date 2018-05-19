
from ..Log import Log
from . import SQL



async def table_setup():
    """Setup any SQL tables needed for this class
    """
    log = Log()
    sql = SQL.SQL()


    log.info("Check to see if users exists.")
    if not await sql.table_exists("users"):
        log.info("Create users table")
        cur = sql.cur
        cmd = """
            CREATE TABLE IF NOT EXISTS users
            (
                name TEXT NOT NULL,
                user_id TEXT NOT NULL,
                discriminator TEXT,
                avatar TEXT,
                bot BOOLEAN,
                avatar_url TEXT,
                default_avatar TEXT,
                default_avatar_url TEXT,
                mention TEXT,
                created_at INTEGER
            )"""
        cur.execute(cmd)
        await sql.commit()


    log.info("Check to see if trainers exists.")
    if not await sql.table_exists("trainers"):
        log.info("Create trainers table")
        cur = sql.cur
        cmd = """
            CREATE TABLE trainers
            (
                trainer_id TEXT NOT NULL UNIQUE,
                user_id TEXT NOT NULL,
                server_id TEXT NOT NULL,
                nickname TEXT,
                created_on TEXT
            )
        """
        cur.execute(cmd)
        await sql.commit()


    log.info("Check to see if trainer_data exists.")
    if not await sql.table_exists("trainer_data"):
        log.info("Create trainer_data table")
        cur = sql.cur
        cmd = """
            CREATE TABLE trainer_data
            (
                trainer_id TEXT NOT NULL UNIQUE,

                state INTEGER DEFAULT 0,

                /* Track where we are.*/
                current_region_id TEXT,
                current_zone_id TEXT,
                current_building_id TEXT,

                /* Track where we want to be, if we are traveling */
                destination_region_id TEXT DEFAULT NULL,
                destination_zone_id TEXT DEFAULT NULL,
                destination_building_id TEXT DEFAULT NULL,
                destination_distance REAL DEFAULT NULL
            )
        """
        cur.execute(cmd)
        await sql.commit()


    log.info("Check to see if trainer_party exists.")
    if not await sql.table_exists("trainer_party"):
        log.info("Create trainer_party table")
        cur = sql.cur
        cmd = """
            CREATE TABLE trainer_party
            (
                trainer_id TEXT NOT NULL UNIQUE,
                monster_id_0 TEXT DEFAULT NULL,
                monster_id_1 TEXT DEFAULT NULL,
                monster_id_2 TEXT DEFAULT NULL,
                monster_id_3 TEXT DEFAULT NULL,
                monster_id_4 TEXT DEFAULT NULL,
                monster_id_5 TEXT DEFAULT NULL
            )
        """
        cur.execute(cmd)
        await sql.commit()


    log.info("Check to see if trainer_stats exists.")
    if not await sql.table_exists("trainer_stats"):
        log.info("Create trainer_stats table")
        cur = sql.cur
        cmd = """
            CREATE TABLE trainer_stats
            (
                trainer_id TEXT NOT NULL UNIQUE,
                pokecoin REAL DEFAULT 100,
                xp INTEGER DEFAULT 0,
                level_bug INTEGER DEFAULT 0,
                level_dark INTEGER DEFAULT 0,
                level_dragon INTEGER DEFAULT 0,
                level_electric INTEGER DEFAULT 0,
                level_fairy INTEGER DEFAULT 0,
                level_fight INTEGER DEFAULT 0,
                level_fire INTEGER DEFAULT 0,
                level_flying INTEGER DEFAULT 0,
                level_ghost INTEGER DEFAULT 0,
                level_grass INTEGER DEFAULT 0,
                level_ground INTEGER DEFAULT 0,
                level_ice INTEGER DEFAULT 0,
                level_normal INTEGER DEFAULT 0,
                level_poison INTEGER DEFAULT 0,
                level_psychic INTEGER DEFAULT 0,
                level_rock INTEGER DEFAULT 0,
                level_steel INTEGER DEFAULT 0,
                level_water INTEGER DEFAULT 0,

                /* Fun player stats */
                commands INTEGER DEFAULT 0,
                steps_taken INTEGER DEFAULT 0,
                meters_biked INTEGER DEFAULT 0,

                pokeballs_thrown INTEGER DEFAULT 0,

                pokemon_caught INTEGER DEFAULT 0,
                pokemon_fainted INTEGER DEFAULT 0,
                pokemon_released INTEGER DEFAULT 0,

                damage_dealt INTEGER DEFAULT 0,
                damage_taken INTEGER DEFAULT 0,
                total_turns INTEGER DEFAULT 0

            )
        """
        cur.execute(cmd)
        await sql.commit()


    log.info("Check to see if trainer_pokedex exists.")
    if not await sql.table_exists("trainer_pokedex"):
        log.info("Create trainer_pokedex table")
        cur = sql.cur
        cmd = """
            CREATE TABLE trainer_pokedex
            (
                trainer_id TEXT NOT NULL UNIQUE,
                pokemon_id TEXT NOT NULL,
                seen INTEGER DEFAULT 0,
                battled INTEGER DEFAULT 0,
                defeated INTEGER DEFAULT 0,
                caught INTEGER DEFAULT 0
            )
        """
        cur.execute(cmd)
        await sql.commit()


    log.info("Check to see if pokedex exists.")
    if not await sql.table_exists("pokedex"):
        log.info("Create pokedex table")
        cur = sql.cur
        cmd = """
            CREATE TABLE pokedex
            (
                pokemon_id TEXT DEFAULT 0,
                identifier TEXT NOT NULL,
                generation_id TEXT,
                evolves_from_species_id TEXT,
                evolution_chain_id TEXT,
                color_id TEXT,
                shape_id TEXT,
                habitat_id TEXT,
                gender_rate INTEGER,
                capture_rate INTEGER,
                base_happiness INTEGER,
                is_baby BOOLEAN,
                hatch_counter INTEGER,
                has_gender_differences BOOLEAN,
                growth_rate_id TEXT,
                forms_switchable BOOLEAN,
                height INTEGER,
                weight INTEGER,
                base_experience INTEGER NOT NULL,
                base_hp INTEGER NOT NULL,
                base_attack INTEGER NOT NULL,
                base_defense INTEGER NOT NULL,
                base_sp_attack INTEGER NOT NULL,
                base_sp_defense INTEGER NOT NULL,
                base_speed INTEGER NOT NULL,
                effort_hp INTEGER NOT NULL,
                effort_attack INTEGER NOT NULL,
                effort_defense INTEGER NOT NULL,
                effort_sp_attack INTEGER NOT NULL,
                effort_sp_defense INTEGER NOT NULL,
                effort_speed INTEGER NOT NULL,
                abilities TEXT,
                hidden_abilities TEXT,
                type1 TEXT NOT NULL,
                type2 TEXT
            )
        """
        cur.execute(cmd)
        await sql.commit()


    log.info("Check to see if moves exists.")
    if not await sql.table_exists("moves"):
        log.info("Create moves table")
        cur = sql.cur
        cmd = """
            CREATE TABLE moves
            (
                move_id TEXT NOT NULL,
                identifier TEXT NOT NULL,
                generation_id INTEGER,
                type_id TEXT NOT NULL,
                power INTEGER,
                pp_max INTEGER,
                accuracy INTEGER,
                priority INTEGER,
                target_id TEXT,
                damage_class_id TEXT,
                effect_id TEXT,
                effect_chance INTEGER,
                contest_type_id TEXT,
                contest_effect_id TEXT,
                super_contest_effect_id TEXT
            )
        """
        cur.execute(cmd)
        await sql.commit()


    log.info("Check to see if pokemon_moves exists.")
    if not await sql.table_exists("pokemon_moves"):
        log.info("Create pokemon_moves table")
        cur = sql.cur
        cmd = """
            CREATE TABLE pokemon_moves
            (
                pokemon_id TEXT NOT NULL,
                version_group_id TEXT NOT NULL,
                move_id TEXT NOT NULL,
                pokemon_move_method_id INTEGER,
                level INTEGER
                --order INTEGER --This is an SQL keyword, and I'm not sure we need it?
            )
        """
        cur.execute(cmd)
        await sql.commit()


    log.info("Check to see if move_effect_prose exists.")
    if not await sql.table_exists("move_effect_prose"):
        log.info("Create move_effect_prose table")
        cur = sql.cur
        cmd = """
            CREATE TABLE move_effect_prose
            (
                effect_id TEXT NOT NULL,
                local_language_id TEXT NOT NULL,
                short_effect TEXT,
                effect TEXT
            )
        """
        cur.execute(cmd)
        await sql.commit()


    log.info("Check to see if pokemon_move_method_prose exists.")
    if not await sql.table_exists("pokemon_move_method_prose"):
        log.info("Create pokemon_move_method_prose table")
        cur = sql.cur
        cmd = """
            CREATE TABLE pokemon_move_method_prose
            (
                pokemon_move_method_id TEXT NOT NULL,
                local_language_id TEXT NOT NULL,
                name TEXT,
                description TEXT
            )
        """
        cur.execute(cmd)
        await sql.commit()



    log.info("Check to see if monsters exists.")
    if not await sql.table_exists("monsters"):
        log.info("Create monsters table")
        cur = sql.cur
        cmd = """
            CREATE TABLE monsters
            (
                monster_id TEXT NOT NULL UNIQUE,
                pokemon_id TEXT NOT NULL,
                name TEXT,
                hp INTEGER NOT NULL,
                attack INTEGER NOT NULL,
                defense INTEGER NOT NULL,
                sp_attack INTEGER NOT NULL,
                sp_defense INTEGER NOT NULL,
                speed INTEGER NOT NULL,
                xp INTEGER NOT NULL,
                ability TEXT,
                hidden_ability TEXT,
                gender TEXT,
                iv_hp INTEGER DEFAULT 0,
                iv_attack INTEGER DEFAULT 0,
                iv_defense INTEGER DEFAULT 0,
                iv_sp_attack INTEGER DEFAULT 0,
                iv_sp_defense INTEGER DEFAULT 0,
                iv_speed INTEGER DEFAULT 0,
                ev_hp INTEGER DEFAULT 0,
                ev_attack INTEGER DEFAULT 0,
                ev_defense INTEGER DEFAULT 0,
                ev_sp_attack INTEGER DEFAULT 0,
                ev_sp_defense INTEGER DEFAULT 0,
                ev_speed INTEGER DEFAULT 0
            )
        """
        cur.execute(cmd)
        await sql.commit()


    log.info("Check to see if move_slots exists.")
    if not await sql.table_exists("move_slots"):
        log.info("Create move_slots table")
        cur = sql.cur
        cmd = """
            CREATE TABLE move_slots
            (
                move_id TEXT NOT NULL,
                move_uuid TEXT NOT NULL,
                slot_number INTEGER NOT NULL,
                pp INTEGER NOT NULL,
                pp_max_slot INTEGER NOT NULL
            )
        """
        cur.execute(cmd)
        await sql.commit()


    log.info("Check to see if types exists.")
    if not await sql.table_exists("types"):
        log.info("Create types table")
        cur = sql.cur
        cmd = """
            CREATE TABLE types
            (
                type_id TEXT NOT NULL,
                identifier TEXT NOT NULL
            )
        """
        cur.execute(cmd)
        await sql.commit()


    log.info("Check to see if type_efficacy exists.")
    if not await sql.table_exists("type_efficacy"):
        log.info("Create type_efficacy table")
        cur = sql.cur
        cmd = """
            CREATE TABLE type_efficacy
            (
                damage_type_id INTEGER NOT NULL,
                target_type_id INTEGER NOT NULL,
                damage_factor INTEGER NOT NULL
            )
        """
        cur.execute(cmd)
        await sql.commit()


    log.info("Check to see if encounters exists.")
    if not await sql.table_exists("encounters"):
        log.info("Create encounters table")
        cur = sql.cur
        cmd = """
            CREATE TABLE encounters
            (
                location_id TEXT NOT NULL,
                encounter_slot_id TEXT,
                location_area_id TEXT,
                max_level INTEGER,
                min_level INTEGER,
                pokemon_id TEXT,
                version_id TEXT
            )
        """
        cur.execute(cmd)
        await sql.commit()


    log.info("Check to see if locations exists.")
    if not await sql.table_exists("locations"):
        log.info("Create location table")
        cur = sql.cur
        cmd = """
            CREATE TABLE locations
            (
                location_id TEXT NOT NULL,
                name TEXT NOT NULL
            )
        """
        cur.execute(cmd)
        await sql.commit()


    log.info("Check to see if zone_connections exists.")
    if not await sql.table_exists("zone_connections"):
        log.info("Create zone_connections table")
        cur = sql.cur
        cmd = """
            CREATE TABLE zone_connections
            (
                location_id_1 TEXT NOT NULL,
                location_id_2 TEXT NOT NULL,
                distance_forward REAL DEFAULT NULL,
                distance_backward REAL DEFAULT NULL
            )
        """
        cur.execute(cmd)
        await sql.commit()


    log.info("Check to see if experience_lookup exists.")
    if not await sql.table_exists("experience_lookup"):
        log.info("Create experience_lookup table")
        cur = sql.cur
        cmd = """
            CREATE TABLE experience_lookup
            (
                growth_rate_id TEXT,
                level INTEGER,
                experience INTEGER
            )
        """
        cur.execute(cmd)
        await sql.commit()
