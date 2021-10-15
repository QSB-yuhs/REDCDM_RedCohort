CREATE TABLE #Codesets (
  codeset_id int NOT NULL,
  concept_id bigint NOT NULL
)
;

INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 13 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (4118993)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (4118993)
  and c.invalid_reason is null
UNION
select distinct cr.concept_id_1 as concept_id
FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (4118993)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (4118993)
  and c.invalid_reason is null

) C
join @vocabulary_database_schema.concept_relationship cr on C.concept_id = cr.concept_id_2 and cr.relationship_id = 'Maps to' and cr.invalid_reason IS NULL

) I
) C UNION ALL 
SELECT 15 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (192273,444463,4181328,4118993,442122)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (192273,444463,4181328,4118993,442122)
  and c.invalid_reason is null
UNION
select distinct cr.concept_id_1 as concept_id
FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (192273,444463,4181328,4118993,442122)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (192273,444463,4181328,4118993,442122)
  and c.invalid_reason is null

) C
join @vocabulary_database_schema.concept_relationship cr on C.concept_id = cr.concept_id_2 and cr.relationship_id = 'Maps to' and cr.invalid_reason IS NULL

) I
) C UNION ALL 
SELECT 16 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (4137462,4327176,4068200,44508882,4286822,4237232,4137133,2002265,40492375,4004044,4232190,4047234,4121769,4001702,4067613,4299725)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (4137462,4327176,4068200,44508882,4286822,4237232,4137133,2002265,40492375,4004044,4232190,4047234,4121769,4001702,4067613,4299725)
  and c.invalid_reason is null
UNION
select distinct cr.concept_id_1 as concept_id
FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (4137462,4327176,4068200,44508882,4286822,4237232,4137133,2002265,40492375,4004044,4232190,4047234,4121769,4001702,4067613,4299725)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (4137462,4327176,4068200,44508882,4286822,4237232,4137133,2002265,40492375,4004044,4232190,4047234,4121769,4001702,4067613,4299725)
  and c.invalid_reason is null

) C
join @vocabulary_database_schema.concept_relationship cr on C.concept_id = cr.concept_id_2 and cr.relationship_id = 'Maps to' and cr.invalid_reason IS NULL

) I
) C UNION ALL 
SELECT 17 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (40340398,37393933,40310520,4193860,4196846,37399298,40309976,40339883,4199030,4199029,4210864,37398755,4310325,40559509,40564451,4198722,37393820,4131370,40285673,4038803,3016181,3036245,44816723,3021777,40759679,3027786,3008237,3008090,3001140,3013490,3036025,3004458,3015380,3001253,3008800,3020865,3017049,3017590,3017891,3004725,3018686,3021343,3005295,3000169,3003109,3001769,3001225,43055676,3011401,3001841,3006214,3011500,40796612,37036222,37063192,40776711,37025648,37043410,3014627,40787157,37039622,4139269,4149410,4148297,3044079,3015397,3044459,3018736,3047199,37054099,40308847,40330697,37394369,4197853,40656094,40656095,40654745,40656096,40654746,40654747,40656097,3017018,3017561,3008625,3009285,3009820,3019075,3020621,3015309,3036376,3009056,3036896,3037433,3020568,3011762,3019312,3021964,3003813,3016225,3028710,3024043,3024590,3011488,3011403,3024753,3033342,3024228,3048732,3052833,3050652,3051683,3049698,3051033,3051993,3049052,3052537,3049733,3050696,3053141,3030508,3036478,3051896,3049367,3052297,3051313,3049050,3052854,40790021,37061564,37024685,37065748,37035303,40793055,37051818,37032904,4274176,4137298,4246956,3005488,3043041,3032145,40778375,37068713,3016767,37054071,40656104,3043826,3008612,3034197,3022794,3043098,37029772,40656100,40656101,37079913,37079914,37079915,37079916,37079917,37079918,37079919,37079920,37079921,37079922,37079923,37079924,37079925,37079926,37079927,37079928,37079929,37079930,40654756,40652957,40656102,40654757,40654758,40656103,44806452,44806453,4038804,40285674,4209238,4193725,4250122,4150618,40653310,40653311,40656294,3019965,3013748,3015200,3011933,3033923,3026576,3009340,3044327,40797098,37075608,40793066,37076220,3046750,3053333,3046509,3053025,3039093,3032593,37076872,37045351,3046603,3052796,3032155,40784271,37072202,40784133,37027951,3043018,40656295,36303464,40770912,37035863,40656296,21491199,43055504,37036622,37053240,46236071,37042175,40760882,37034339,37038287,40655088,3036105,3016716,3048826,3033458,37068165,40656297,40656298,40655089,40656299,40653312,40656300,40655090,40655091,40656301,3006636,3002456,3015737,3006956,3012403,42869546,3012602,3021531,40777273,37072714,40780074,37031791,4101237,3029192,3011452,3018694,37047858,40656302,40655092,40656303,40653313,40655093,40655094,40656304,3011657,3037760,3022417,3010337,3005537,3027524,3028578,3004442,3015025,3009891,3003273,3008553,3001610,3016161,3016702,3021527,3012414,40760774,3010429,3036359,3026923,3013062,3036879,3004317,3003219,3002055,3035576,3037236,3016989,3023005,3028235,3007706,3049046,3048734,3048731,3049731,3051346,3051886,3050046,3052574,3053162,3051909,3052188,3048416,3053206,3049534,3051990,3030937,3030633,3029607,3006382,3050971,3050991,3052886,3051638,3053182,3052305,40773459,37047281,37058941,37026140,40789960,37023966,37033739,4148073,4059909,4259269,3046924,3019797,3013485,3013526,3027085,37077030,40656331,40656332,37080154,37080155,37080156,37080157,37080158,37080159,37080160,37080161,37080162,37080163,37080164,37080165,37080166,37080167,37080168,37080169,37080170,37080171,37080172,37080173,37080174,37080175,37080176,40655195,40656333,40655196,40655197,40656334,3035015,3003700,3016851,3034481,3008389,3000213,3009588,3026179,3045191,40773913,37029212,40793222,37028062,40760574,3042027,40781044,37072160,4017218,40655202,3044199,3005474,3045317,3045200,37064754,40656335,40656336,40655203,40656337,40655204,40655205,40656338,4209252,4209397,37397985,4260775,4209232,4195198,40339875,40309969,37392368,37398445,44811466,4150616,40339889,40309981,37393647,4198112,4210866,4209386,4150617,37398609,40308854,40330705,4196839,40309971,37393642,40782620,3025591,3002374,3022684,3019414,3023112,3024689,44816955,3009964,40777421,37043456,37039213,40780175,37078295,3030271,37025443,3037649,37032984,37060187,1175254,37031232,37044373,3049524,37051987,37054593,40769425,37028896,37069478,3044702,3026980,40759764,37027104,40656522,37026691,3015126,3027821,3011226,3019848,37040531,40656523,40656524,40655619,40656525,40655620,40656526,4304698,40628894,4231897,44788712,40339877,4231896)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (40340398,37393933,40310520,4193860,4196846,37399298,40309976,40339883,4199030,4199029,4210864,37398755,4310325,40559509,40564451,4198722,37393820,4131370,40285673,4038803,3016181,3036245,44816723,3021777,40759679,3027786,3008237,3008090,3001140,3013490,3036025,3004458,3015380,3001253,3008800,3020865,3017049,3017590,3017891,3004725,3018686,3021343,3005295,3000169,3003109,3001769,3001225,43055676,3011401,3001841,3006214,3011500,40796612,37036222,37063192,40776711,37025648,37043410,3014627,40787157,37039622,4139269,4149410,4148297,3044079,3015397,3044459,3018736,3047199,37054099,40308847,40330697,37394369,4197853,40656094,40656095,40654745,40656096,40654746,40654747,40656097,3017018,3017561,3008625,3009285,3009820,3019075,3020621,3015309,3036376,3009056,3036896,3037433,3020568,3011762,3019312,3021964,3003813,3016225,3028710,3024043,3024590,3011488,3011403,3024753,3033342,3024228,3048732,3052833,3050652,3051683,3049698,3051033,3051993,3049052,3052537,3049733,3050696,3053141,3030508,3036478,3051896,3049367,3052297,3051313,3049050,3052854,40790021,37061564,37024685,37065748,37035303,40793055,37051818,37032904,4274176,4137298,4246956,3005488,3043041,3032145,40778375,37068713,3016767,37054071,40656104,3043826,3008612,3034197,3022794,3043098,37029772,40656100,40656101,37079913,37079914,37079915,37079916,37079917,37079918,37079919,37079920,37079921,37079922,37079923,37079924,37079925,37079926,37079927,37079928,37079929,37079930,40654756,40652957,40656102,40654757,40654758,40656103,44806452,44806453,4038804,40285674,4209238,4193725,4250122,4150618,40653310,40653311,40656294,3019965,3013748,3015200,3011933,3033923,3026576,3009340,3044327,40797098,37075608,40793066,37076220,3046750,3053333,3046509,3053025,3039093,3032593,37076872,37045351,3046603,3052796,3032155,40784271,37072202,40784133,37027951,3043018,40656295,36303464,40770912,37035863,40656296,21491199,43055504,37036622,37053240,46236071,37042175,40760882,37034339,37038287,40655088,3036105,3016716,3048826,3033458,37068165,40656297,40656298,40655089,40656299,40653312,40656300,40655090,40655091,40656301,3006636,3002456,3015737,3006956,3012403,42869546,3012602,3021531,40777273,37072714,40780074,37031791,4101237,3029192,3011452,3018694,37047858,40656302,40655092,40656303,40653313,40655093,40655094,40656304,3011657,3037760,3022417,3010337,3005537,3027524,3028578,3004442,3015025,3009891,3003273,3008553,3001610,3016161,3016702,3021527,3012414,40760774,3010429,3036359,3026923,3013062,3036879,3004317,3003219,3002055,3035576,3037236,3016989,3023005,3028235,3007706,3049046,3048734,3048731,3049731,3051346,3051886,3050046,3052574,3053162,3051909,3052188,3048416,3053206,3049534,3051990,3030937,3030633,3029607,3006382,3050971,3050991,3052886,3051638,3053182,3052305,40773459,37047281,37058941,37026140,40789960,37023966,37033739,4148073,4059909,4259269,3046924,3019797,3013485,3013526,3027085,37077030,40656331,40656332,37080154,37080155,37080156,37080157,37080158,37080159,37080160,37080161,37080162,37080163,37080164,37080165,37080166,37080167,37080168,37080169,37080170,37080171,37080172,37080173,37080174,37080175,37080176,40655195,40656333,40655196,40655197,40656334,3035015,3003700,3016851,3034481,3008389,3000213,3009588,3026179,3045191,40773913,37029212,40793222,37028062,40760574,3042027,40781044,37072160,4017218,40655202,3044199,3005474,3045317,3045200,37064754,40656335,40656336,40655203,40656337,40655204,40655205,40656338,4209252,4209397,37397985,4260775,4209232,4195198,40339875,40309969,37392368,37398445,44811466,4150616,40339889,40309981,37393647,4198112,4210866,4209386,4150617,37398609,40308854,40330705,4196839,40309971,37393642,40782620,3025591,3002374,3022684,3019414,3023112,3024689,44816955,3009964,40777421,37043456,37039213,40780175,37078295,3030271,37025443,3037649,37032984,37060187,1175254,37031232,37044373,3049524,37051987,37054593,40769425,37028896,37069478,3044702,3026980,40759764,37027104,40656522,37026691,3015126,3027821,3011226,3019848,37040531,40656523,40656524,40655619,40656525,40655620,40656526,4304698,40628894,4231897,44788712,40339877,4231896)
  and c.invalid_reason is null
UNION
select distinct cr.concept_id_1 as concept_id
FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (40340398,37393933,40310520,4193860,4196846,37399298,40309976,40339883,4199030,4199029,4210864,37398755,4310325,40559509,40564451,4198722,37393820,4131370,40285673,4038803,3016181,3036245,44816723,3021777,40759679,3027786,3008237,3008090,3001140,3013490,3036025,3004458,3015380,3001253,3008800,3020865,3017049,3017590,3017891,3004725,3018686,3021343,3005295,3000169,3003109,3001769,3001225,43055676,3011401,3001841,3006214,3011500,40796612,37036222,37063192,40776711,37025648,37043410,3014627,40787157,37039622,4139269,4149410,4148297,3044079,3015397,3044459,3018736,3047199,37054099,40308847,40330697,37394369,4197853,40656094,40656095,40654745,40656096,40654746,40654747,40656097,3017018,3017561,3008625,3009285,3009820,3019075,3020621,3015309,3036376,3009056,3036896,3037433,3020568,3011762,3019312,3021964,3003813,3016225,3028710,3024043,3024590,3011488,3011403,3024753,3033342,3024228,3048732,3052833,3050652,3051683,3049698,3051033,3051993,3049052,3052537,3049733,3050696,3053141,3030508,3036478,3051896,3049367,3052297,3051313,3049050,3052854,40790021,37061564,37024685,37065748,37035303,40793055,37051818,37032904,4274176,4137298,4246956,3005488,3043041,3032145,40778375,37068713,3016767,37054071,40656104,3043826,3008612,3034197,3022794,3043098,37029772,40656100,40656101,37079913,37079914,37079915,37079916,37079917,37079918,37079919,37079920,37079921,37079922,37079923,37079924,37079925,37079926,37079927,37079928,37079929,37079930,40654756,40652957,40656102,40654757,40654758,40656103,44806452,44806453,4038804,40285674,4209238,4193725,4250122,4150618,40653310,40653311,40656294,3019965,3013748,3015200,3011933,3033923,3026576,3009340,3044327,40797098,37075608,40793066,37076220,3046750,3053333,3046509,3053025,3039093,3032593,37076872,37045351,3046603,3052796,3032155,40784271,37072202,40784133,37027951,3043018,40656295,36303464,40770912,37035863,40656296,21491199,43055504,37036622,37053240,46236071,37042175,40760882,37034339,37038287,40655088,3036105,3016716,3048826,3033458,37068165,40656297,40656298,40655089,40656299,40653312,40656300,40655090,40655091,40656301,3006636,3002456,3015737,3006956,3012403,42869546,3012602,3021531,40777273,37072714,40780074,37031791,4101237,3029192,3011452,3018694,37047858,40656302,40655092,40656303,40653313,40655093,40655094,40656304,3011657,3037760,3022417,3010337,3005537,3027524,3028578,3004442,3015025,3009891,3003273,3008553,3001610,3016161,3016702,3021527,3012414,40760774,3010429,3036359,3026923,3013062,3036879,3004317,3003219,3002055,3035576,3037236,3016989,3023005,3028235,3007706,3049046,3048734,3048731,3049731,3051346,3051886,3050046,3052574,3053162,3051909,3052188,3048416,3053206,3049534,3051990,3030937,3030633,3029607,3006382,3050971,3050991,3052886,3051638,3053182,3052305,40773459,37047281,37058941,37026140,40789960,37023966,37033739,4148073,4059909,4259269,3046924,3019797,3013485,3013526,3027085,37077030,40656331,40656332,37080154,37080155,37080156,37080157,37080158,37080159,37080160,37080161,37080162,37080163,37080164,37080165,37080166,37080167,37080168,37080169,37080170,37080171,37080172,37080173,37080174,37080175,37080176,40655195,40656333,40655196,40655197,40656334,3035015,3003700,3016851,3034481,3008389,3000213,3009588,3026179,3045191,40773913,37029212,40793222,37028062,40760574,3042027,40781044,37072160,4017218,40655202,3044199,3005474,3045317,3045200,37064754,40656335,40656336,40655203,40656337,40655204,40655205,40656338,4209252,4209397,37397985,4260775,4209232,4195198,40339875,40309969,37392368,37398445,44811466,4150616,40339889,40309981,37393647,4198112,4210866,4209386,4150617,37398609,40308854,40330705,4196839,40309971,37393642,40782620,3025591,3002374,3022684,3019414,3023112,3024689,44816955,3009964,40777421,37043456,37039213,40780175,37078295,3030271,37025443,3037649,37032984,37060187,1175254,37031232,37044373,3049524,37051987,37054593,40769425,37028896,37069478,3044702,3026980,40759764,37027104,40656522,37026691,3015126,3027821,3011226,3019848,37040531,40656523,40656524,40655619,40656525,40655620,40656526,4304698,40628894,4231897,44788712,40339877,4231896)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (40340398,37393933,40310520,4193860,4196846,37399298,40309976,40339883,4199030,4199029,4210864,37398755,4310325,40559509,40564451,4198722,37393820,4131370,40285673,4038803,3016181,3036245,44816723,3021777,40759679,3027786,3008237,3008090,3001140,3013490,3036025,3004458,3015380,3001253,3008800,3020865,3017049,3017590,3017891,3004725,3018686,3021343,3005295,3000169,3003109,3001769,3001225,43055676,3011401,3001841,3006214,3011500,40796612,37036222,37063192,40776711,37025648,37043410,3014627,40787157,37039622,4139269,4149410,4148297,3044079,3015397,3044459,3018736,3047199,37054099,40308847,40330697,37394369,4197853,40656094,40656095,40654745,40656096,40654746,40654747,40656097,3017018,3017561,3008625,3009285,3009820,3019075,3020621,3015309,3036376,3009056,3036896,3037433,3020568,3011762,3019312,3021964,3003813,3016225,3028710,3024043,3024590,3011488,3011403,3024753,3033342,3024228,3048732,3052833,3050652,3051683,3049698,3051033,3051993,3049052,3052537,3049733,3050696,3053141,3030508,3036478,3051896,3049367,3052297,3051313,3049050,3052854,40790021,37061564,37024685,37065748,37035303,40793055,37051818,37032904,4274176,4137298,4246956,3005488,3043041,3032145,40778375,37068713,3016767,37054071,40656104,3043826,3008612,3034197,3022794,3043098,37029772,40656100,40656101,37079913,37079914,37079915,37079916,37079917,37079918,37079919,37079920,37079921,37079922,37079923,37079924,37079925,37079926,37079927,37079928,37079929,37079930,40654756,40652957,40656102,40654757,40654758,40656103,44806452,44806453,4038804,40285674,4209238,4193725,4250122,4150618,40653310,40653311,40656294,3019965,3013748,3015200,3011933,3033923,3026576,3009340,3044327,40797098,37075608,40793066,37076220,3046750,3053333,3046509,3053025,3039093,3032593,37076872,37045351,3046603,3052796,3032155,40784271,37072202,40784133,37027951,3043018,40656295,36303464,40770912,37035863,40656296,21491199,43055504,37036622,37053240,46236071,37042175,40760882,37034339,37038287,40655088,3036105,3016716,3048826,3033458,37068165,40656297,40656298,40655089,40656299,40653312,40656300,40655090,40655091,40656301,3006636,3002456,3015737,3006956,3012403,42869546,3012602,3021531,40777273,37072714,40780074,37031791,4101237,3029192,3011452,3018694,37047858,40656302,40655092,40656303,40653313,40655093,40655094,40656304,3011657,3037760,3022417,3010337,3005537,3027524,3028578,3004442,3015025,3009891,3003273,3008553,3001610,3016161,3016702,3021527,3012414,40760774,3010429,3036359,3026923,3013062,3036879,3004317,3003219,3002055,3035576,3037236,3016989,3023005,3028235,3007706,3049046,3048734,3048731,3049731,3051346,3051886,3050046,3052574,3053162,3051909,3052188,3048416,3053206,3049534,3051990,3030937,3030633,3029607,3006382,3050971,3050991,3052886,3051638,3053182,3052305,40773459,37047281,37058941,37026140,40789960,37023966,37033739,4148073,4059909,4259269,3046924,3019797,3013485,3013526,3027085,37077030,40656331,40656332,37080154,37080155,37080156,37080157,37080158,37080159,37080160,37080161,37080162,37080163,37080164,37080165,37080166,37080167,37080168,37080169,37080170,37080171,37080172,37080173,37080174,37080175,37080176,40655195,40656333,40655196,40655197,40656334,3035015,3003700,3016851,3034481,3008389,3000213,3009588,3026179,3045191,40773913,37029212,40793222,37028062,40760574,3042027,40781044,37072160,4017218,40655202,3044199,3005474,3045317,3045200,37064754,40656335,40656336,40655203,40656337,40655204,40655205,40656338,4209252,4209397,37397985,4260775,4209232,4195198,40339875,40309969,37392368,37398445,44811466,4150616,40339889,40309981,37393647,4198112,4210866,4209386,4150617,37398609,40308854,40330705,4196839,40309971,37393642,40782620,3025591,3002374,3022684,3019414,3023112,3024689,44816955,3009964,40777421,37043456,37039213,40780175,37078295,3030271,37025443,3037649,37032984,37060187,1175254,37031232,37044373,3049524,37051987,37054593,40769425,37028896,37069478,3044702,3026980,40759764,37027104,40656522,37026691,3015126,3027821,3011226,3019848,37040531,40656523,40656524,40655619,40656525,40655620,40656526,4304698,40628894,4231897,44788712,40339877,4231896)
  and c.invalid_reason is null

) C
join @vocabulary_database_schema.concept_relationship cr on C.concept_id = cr.concept_id_2 and cr.relationship_id = 'Maps to' and cr.invalid_reason IS NULL

) I
) C UNION ALL 
SELECT 18 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (434000)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (434000)
  and c.invalid_reason is null
UNION
select distinct cr.concept_id_1 as concept_id
FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (434000)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (434000)
  and c.invalid_reason is null

) C
join @vocabulary_database_schema.concept_relationship cr on C.concept_id = cr.concept_id_2 and cr.relationship_id = 'Maps to' and cr.invalid_reason IS NULL

) I
) C UNION ALL 
SELECT 19 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (195212)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (195212)
  and c.invalid_reason is null
UNION
select distinct cr.concept_id_1 as concept_id
FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (195212)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (195212)
  and c.invalid_reason is null

) C
join @vocabulary_database_schema.concept_relationship cr on C.concept_id = cr.concept_id_2 and cr.relationship_id = 'Maps to' and cr.invalid_reason IS NULL

) I
) C UNION ALL 
SELECT 20 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (4162115)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (4162115)
  and c.invalid_reason is null
UNION
select distinct cr.concept_id_1 as concept_id
FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (4162115)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (4162115)
  and c.invalid_reason is null

) C
join @vocabulary_database_schema.concept_relationship cr on C.concept_id = cr.concept_id_2 and cr.relationship_id = 'Maps to' and cr.invalid_reason IS NULL

) I
) C
;

with primary_events (event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id) as
(
-- Begin Primary Events
select P.ordinal as event_id, P.person_id, P.start_date, P.end_date, op_start_date, op_end_date, cast(P.visit_occurrence_id as bigint) as visit_occurrence_id
FROM
(
  select E.person_id, E.start_date, E.end_date,
         row_number() OVER (PARTITION BY E.person_id ORDER BY E.sort_date ASC) ordinal,
         OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date, cast(E.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM 
  (
  -- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, DATEADD(day,1,C.condition_start_date)) as end_date,
  C.visit_occurrence_id, C.condition_start_date as sort_date
FROM 
(
  SELECT co.* 
  FROM @cdm_database_schema.CONDITION_OCCURRENCE co
  JOIN #Codesets codesets on ((co.condition_concept_id = codesets.concept_id and codesets.codeset_id = 15))
) C


-- End Condition Occurrence Criteria

  ) E
	JOIN @cdm_database_schema.observation_period OP on E.person_id = OP.person_id and E.start_date >=  OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
  WHERE DATEADD(day,0,OP.OBSERVATION_PERIOD_START_DATE) <= E.START_DATE AND DATEADD(day,0,E.START_DATE) <= OP.OBSERVATION_PERIOD_END_DATE
) P
WHERE P.ordinal = 1
-- End Primary Events

)
SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id
INTO #qualified_events
FROM 
(
  select pe.event_id, pe.person_id, pe.start_date, pe.end_date, pe.op_start_date, pe.op_end_date, row_number() over (partition by pe.person_id order by pe.start_date ASC) as ordinal, cast(pe.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM primary_events pe
  
) QE

;

--- Inclusion Rule Inserts

select 0 as inclusion_rule_id, person_id, event_id
INTO #Inclusion_0
FROM 
(
  select pe.person_id, pe.event_id
  FROM #qualified_events pe
  
JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id 
  FROM #qualified_events E
  INNER JOIN
  (
    -- Begin Correlated Criteria
select 0 as index_id, cc.person_id, cc.event_id
from (SELECT p.person_id, p.event_id 
FROM #qualified_events P
JOIN (
  -- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, DATEADD(day,1,C.condition_start_date)) as end_date,
  C.visit_occurrence_id, C.condition_start_date as sort_date
FROM 
(
  SELECT co.* 
  FROM @cdm_database_schema.CONDITION_OCCURRENCE co
  JOIN #Codesets codesets on ((co.condition_concept_id = codesets.concept_id and codesets.codeset_id = 15))
) C


-- End Condition Occurrence Criteria

) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= DATEADD(day,-1,P.START_DATE) AND A.START_DATE <= P.OP_END_DATE ) cc 
GROUP BY cc.person_id, cc.event_id
HAVING COUNT(cc.event_id) >= 1
-- End Correlated Criteria

  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 1
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;

select 1 as inclusion_rule_id, person_id, event_id
INTO #Inclusion_1
FROM 
(
  select pe.person_id, pe.event_id
  FROM #qualified_events pe
  
JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id 
  FROM #qualified_events E
  INNER JOIN
  (
    -- Begin Correlated Criteria
select 0 as index_id, cc.person_id, cc.event_id
from (SELECT p.person_id, p.event_id 
FROM #qualified_events P
JOIN (
  select PE.person_id, PE.event_id, PE.start_date, PE.end_date, PE.visit_occurrence_id, PE.sort_date FROM (
-- Begin Procedure Occurrence Criteria
select C.person_id, C.procedure_occurrence_id as event_id, C.procedure_date as start_date, DATEADD(d,1,C.procedure_date) as END_DATE,
       C.visit_occurrence_id, C.procedure_date as sort_date
from 
(
  select po.* 
  FROM @cdm_database_schema.PROCEDURE_OCCURRENCE po
JOIN #Codesets codesets on ((po.procedure_concept_id = codesets.concept_id and codesets.codeset_id = 16))
) C


-- End Procedure Occurrence Criteria

) PE
JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id 
  FROM (SELECT Q.person_id, Q.event_id, Q.start_date, Q.end_date, Q.visit_occurrence_id, OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date
FROM (-- Begin Procedure Occurrence Criteria
select C.person_id, C.procedure_occurrence_id as event_id, C.procedure_date as start_date, DATEADD(d,1,C.procedure_date) as END_DATE,
       C.visit_occurrence_id, C.procedure_date as sort_date
from 
(
  select po.* 
  FROM @cdm_database_schema.PROCEDURE_OCCURRENCE po
JOIN #Codesets codesets on ((po.procedure_concept_id = codesets.concept_id and codesets.codeset_id = 16))
) C


-- End Procedure Occurrence Criteria
) Q
JOIN @cdm_database_schema.OBSERVATION_PERIOD OP on Q.person_id = OP.person_id 
  and OP.observation_period_start_date <= Q.start_date and OP.observation_period_end_date >= Q.start_date
) E
  INNER JOIN
  (
    -- Begin Correlated Criteria
select 0 as index_id, cc.person_id, cc.event_id
from (SELECT p.person_id, p.event_id 
FROM (SELECT Q.person_id, Q.event_id, Q.start_date, Q.end_date, Q.visit_occurrence_id, OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date
FROM (-- Begin Procedure Occurrence Criteria
select C.person_id, C.procedure_occurrence_id as event_id, C.procedure_date as start_date, DATEADD(d,1,C.procedure_date) as END_DATE,
       C.visit_occurrence_id, C.procedure_date as sort_date
from 
(
  select po.* 
  FROM @cdm_database_schema.PROCEDURE_OCCURRENCE po
JOIN #Codesets codesets on ((po.procedure_concept_id = codesets.concept_id and codesets.codeset_id = 16))
) C


-- End Procedure Occurrence Criteria
) Q
JOIN @cdm_database_schema.OBSERVATION_PERIOD OP on Q.person_id = OP.person_id 
  and OP.observation_period_start_date <= Q.start_date and OP.observation_period_end_date >= Q.start_date
) P
JOIN (
  -- Begin Measurement Criteria
select C.person_id, C.measurement_id as event_id, C.measurement_date as start_date, DATEADD(d,1,C.measurement_date) as END_DATE,
       C.visit_occurrence_id, C.measurement_date as sort_date
from 
(
  select m.* 
  FROM @cdm_database_schema.MEASUREMENT m
JOIN #Codesets codesets on ((m.measurement_concept_id = codesets.concept_id and codesets.codeset_id = 17))
) C


-- End Measurement Criteria

) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE ) cc 
GROUP BY cc.person_id, cc.event_id
HAVING COUNT(cc.event_id) >= 2
-- End Correlated Criteria

UNION ALL
-- Begin Correlated Criteria
select 1 as index_id, cc.person_id, cc.event_id
from (SELECT p.person_id, p.event_id 
FROM (SELECT Q.person_id, Q.event_id, Q.start_date, Q.end_date, Q.visit_occurrence_id, OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date
FROM (-- Begin Procedure Occurrence Criteria
select C.person_id, C.procedure_occurrence_id as event_id, C.procedure_date as start_date, DATEADD(d,1,C.procedure_date) as END_DATE,
       C.visit_occurrence_id, C.procedure_date as sort_date
from 
(
  select po.* 
  FROM @cdm_database_schema.PROCEDURE_OCCURRENCE po
JOIN #Codesets codesets on ((po.procedure_concept_id = codesets.concept_id and codesets.codeset_id = 16))
) C


-- End Procedure Occurrence Criteria
) Q
JOIN @cdm_database_schema.OBSERVATION_PERIOD OP on Q.person_id = OP.person_id 
  and OP.observation_period_start_date <= Q.start_date and OP.observation_period_end_date >= Q.start_date
) P
JOIN (
  -- Begin Measurement Criteria
select C.person_id, C.measurement_id as event_id, C.measurement_date as start_date, DATEADD(d,1,C.measurement_date) as END_DATE,
       C.visit_occurrence_id, C.measurement_date as sort_date
from 
(
  select m.* 
  FROM @cdm_database_schema.MEASUREMENT m
JOIN #Codesets codesets on ((m.measurement_concept_id = codesets.concept_id and codesets.codeset_id = 17))
) C


-- End Measurement Criteria

) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= DATEADD(day,0,P.START_DATE) ) cc 
GROUP BY cc.person_id, cc.event_id
HAVING COUNT(cc.event_id) >= 1
-- End Correlated Criteria

  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 2
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id and AC.event_id = pe.event_id

) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= DATEADD(day,0,P.START_DATE) AND A.START_DATE <= P.OP_END_DATE ) cc 
GROUP BY cc.person_id, cc.event_id
HAVING COUNT(cc.event_id) >= 1
-- End Correlated Criteria

  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 1
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;

select 2 as inclusion_rule_id, person_id, event_id
INTO #Inclusion_2
FROM 
(
  select pe.person_id, pe.event_id
  FROM #qualified_events pe
  
JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id 
  FROM #qualified_events E
  LEFT JOIN
  (
    -- Begin Correlated Criteria
select 0 as index_id, cc.person_id, cc.event_id
from (SELECT p.person_id, p.event_id 
FROM #qualified_events P
JOIN (
  -- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, DATEADD(day,1,C.condition_start_date)) as end_date,
  C.visit_occurrence_id, C.condition_start_date as sort_date
FROM 
(
  SELECT co.* 
  FROM @cdm_database_schema.CONDITION_OCCURRENCE co
  JOIN #Codesets codesets on ((co.condition_concept_id = codesets.concept_id and codesets.codeset_id = 18))
) C


-- End Condition Occurrence Criteria

) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE ) cc 
GROUP BY cc.person_id, cc.event_id
HAVING COUNT(cc.event_id) >= 1
-- End Correlated Criteria

  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) <= 0
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;

select 3 as inclusion_rule_id, person_id, event_id
INTO #Inclusion_3
FROM 
(
  select pe.person_id, pe.event_id
  FROM #qualified_events pe
  
JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id 
  FROM #qualified_events E
  INNER JOIN
  (
    -- Begin Correlated Criteria
select 0 as index_id, p.person_id, p.event_id
from #qualified_events p
LEFT JOIN (
SELECT p.person_id, p.event_id 
FROM #qualified_events P
JOIN (
  -- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, DATEADD(day,1,C.condition_start_date)) as end_date,
  C.visit_occurrence_id, C.condition_start_date as sort_date
FROM 
(
  SELECT co.* 
  FROM @cdm_database_schema.CONDITION_OCCURRENCE co
  JOIN #Codesets codesets on ((co.condition_concept_id = codesets.concept_id and codesets.codeset_id = 19))
) C


-- End Condition Occurrence Criteria

) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
GROUP BY p.person_id, p.event_id
HAVING COUNT(cc.event_id) <= 0
-- End Correlated Criteria

  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 1
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;

select 4 as inclusion_rule_id, person_id, event_id
INTO #Inclusion_4
FROM 
(
  select pe.person_id, pe.event_id
  FROM #qualified_events pe
  
JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id 
  FROM #qualified_events E
  INNER JOIN
  (
    -- Begin Correlated Criteria
select 0 as index_id, p.person_id, p.event_id
from #qualified_events p
LEFT JOIN (
SELECT p.person_id, p.event_id 
FROM #qualified_events P
JOIN (
  -- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, DATEADD(day,1,C.condition_start_date)) as end_date,
  C.visit_occurrence_id, C.condition_start_date as sort_date
FROM 
(
  SELECT co.* 
  FROM @cdm_database_schema.CONDITION_OCCURRENCE co
  JOIN #Codesets codesets on ((co.condition_concept_id = codesets.concept_id and codesets.codeset_id = 20))
) C


-- End Condition Occurrence Criteria

) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
GROUP BY p.person_id, p.event_id
HAVING COUNT(cc.event_id) <= 0
-- End Correlated Criteria

  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 1
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;

select 5 as inclusion_rule_id, person_id, event_id
INTO #Inclusion_5
FROM 
(
  select pe.person_id, pe.event_id
  FROM #qualified_events pe
  
JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id 
  FROM #qualified_events E
  INNER JOIN
  (
    -- Begin Correlated Criteria
select 0 as index_id, cc.person_id, cc.event_id
from (SELECT p.person_id, p.event_id , A.start_date
FROM #qualified_events P
JOIN (
  -- Begin Measurement Criteria
select C.person_id, C.measurement_id as event_id, C.measurement_date as start_date, DATEADD(d,1,C.measurement_date) as END_DATE,
       C.visit_occurrence_id, C.measurement_date as sort_date
from 
(
  select m.* 
  FROM @cdm_database_schema.MEASUREMENT m
JOIN #Codesets codesets on ((m.measurement_concept_id = codesets.concept_id and codesets.codeset_id = 17))
) C


-- End Measurement Criteria

) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE ) cc 
GROUP BY cc.person_id, cc.event_id
HAVING COUNT(DISTINCT cc.start_date) >= 2
-- End Correlated Criteria

  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 1
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;

SELECT inclusion_rule_id, person_id, event_id
INTO #inclusion_events
FROM (select inclusion_rule_id, person_id, event_id from #Inclusion_0
UNION ALL
select inclusion_rule_id, person_id, event_id from #Inclusion_1
UNION ALL
select inclusion_rule_id, person_id, event_id from #Inclusion_2
UNION ALL
select inclusion_rule_id, person_id, event_id from #Inclusion_3
UNION ALL
select inclusion_rule_id, person_id, event_id from #Inclusion_4
UNION ALL
select inclusion_rule_id, person_id, event_id from #Inclusion_5) I;
TRUNCATE TABLE #Inclusion_0;
DROP TABLE #Inclusion_0;

TRUNCATE TABLE #Inclusion_1;
DROP TABLE #Inclusion_1;

TRUNCATE TABLE #Inclusion_2;
DROP TABLE #Inclusion_2;

TRUNCATE TABLE #Inclusion_3;
DROP TABLE #Inclusion_3;

TRUNCATE TABLE #Inclusion_4;
DROP TABLE #Inclusion_4;

TRUNCATE TABLE #Inclusion_5;
DROP TABLE #Inclusion_5;


with cteIncludedEvents(event_id, person_id, start_date, end_date, op_start_date, op_end_date, ordinal) as
(
  SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, row_number() over (partition by person_id order by start_date ASC) as ordinal
  from
  (
    select Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date, SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) as inclusion_rule_mask
    from #qualified_events Q
    LEFT JOIN #inclusion_events I on I.person_id = Q.person_id and I.event_id = Q.event_id
    GROUP BY Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date
  ) MG -- matching groups

  -- the matching group with all bits set ( POWER(2,# of inclusion rules) - 1 = inclusion_rule_mask
  WHERE (MG.inclusion_rule_mask = POWER(cast(2 as bigint),6)-1)

)
select event_id, person_id, start_date, end_date, op_start_date, op_end_date
into #included_events
FROM cteIncludedEvents Results
WHERE Results.ordinal = 1
;



-- generate cohort periods into #final_cohort
with cohort_ends (event_id, person_id, end_date) as
(
	-- cohort exit dates
  -- By default, cohort exit at the event's op end date
select event_id, person_id, op_end_date as end_date from #included_events
),
first_ends (person_id, start_date, end_date) as
(
	select F.person_id, F.start_date, F.end_date
	FROM (
	  select I.event_id, I.person_id, I.start_date, E.end_date, row_number() over (partition by I.person_id, I.event_id order by E.end_date) as ordinal 
	  from #included_events I
	  join cohort_ends E on I.event_id = E.event_id and I.person_id = E.person_id and E.end_date >= I.start_date
	) F
	WHERE F.ordinal = 1
)
select person_id, start_date, end_date
INTO #cohort_rows
from first_ends;

with cteEndDates (person_id, end_date) AS -- the magic
(	
	SELECT
		person_id
		, DATEADD(day,-1 * 0, event_date)  as end_date
	FROM
	(
		SELECT
			person_id
			, event_date
			, event_type
			, MAX(start_ordinal) OVER (PARTITION BY person_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal 
			, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY event_date, event_type) AS overall_ord
		FROM
		(
			SELECT
				person_id
				, start_date AS event_date
				, -1 AS event_type
				, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY start_date) AS start_ordinal
			FROM #cohort_rows
		
			UNION ALL
		

			SELECT
				person_id
				, DATEADD(day,0,end_date) as end_date
				, 1 AS event_type
				, NULL
			FROM #cohort_rows
		) RAWDATA
	) e
	WHERE (2 * e.start_ordinal) - e.overall_ord = 0
),
cteEnds (person_id, start_date, end_date) AS
(
	SELECT
		 c.person_id
		, c.start_date
		, MIN(e.end_date) AS end_date
	FROM #cohort_rows c
	JOIN cteEndDates e ON c.person_id = e.person_id AND e.end_date >= c.start_date
	GROUP BY c.person_id, c.start_date
)
select person_id, min(start_date) as start_date, end_date
into #final_cohort
from cteEnds
group by person_id, end_date
;

DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;
INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select @target_cohort_id as cohort_definition_id, person_id, start_date, end_date 
FROM #final_cohort CO
;





TRUNCATE TABLE #cohort_rows;
DROP TABLE #cohort_rows;

TRUNCATE TABLE #final_cohort;
DROP TABLE #final_cohort;

TRUNCATE TABLE #inclusion_events;
DROP TABLE #inclusion_events;

TRUNCATE TABLE #qualified_events;
DROP TABLE #qualified_events;

TRUNCATE TABLE #included_events;
DROP TABLE #included_events;

TRUNCATE TABLE #Codesets;
DROP TABLE #Codesets;
