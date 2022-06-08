from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName('Vibration Fault') \
    .getOrCreate()

# Creating a Dictionary with all possible faults
Dict = {}
Dict['count'] = 1 
Dict['unbalance'] = 0
Dict['ecentricity'] = 0
Dict['loose type A'] = 0
Dict['gear tooth'] = 0
Dict['overhang unbalance'] = 0
Dict['belt drive misalignment'] = 0
Dict['misalignment offset'] = 0
Dict['cocked bearing inner'] = 0
Dict['cocked bearing outer'] = 0
Dict['bent rotors'] = 0
Dict['misalignment angular'] = 0
Dict['Gear mesh'] = 0
Dict['rotor bar'] = 0
Dict['Blade Pass'] = 0
Dict['Loose type B'] = 0
Dict['Rub Event'] = 0
Dict['Oil Whirl/Whip'] = 0
Dict['Belt Drive Frequency'] = 0
Dict['Rolling Element Bearing Cage Frequency'] = 0
Dict['Loose Type C'] = 0
Dict['Gear Tooth hunting'] = 0
Dict['assembly'] = 0
Dict['Flow turbulance/cavitation'] = 0
Dict['electrical pole'] = 0
Dict['Rolling Element Bearing outer race'] = 0
Dict['Rolling Element Bearing inner race'] = 0

def fault_detect(q_r , r_r , q_a ,r_a):
    Dict['count'] = +1
    if(q_r == 1 and r_r == 0):
        #fault.extend([0,1,2,3])
        Dict['unbalance'] = Dict['unbalance']+1
        Dict['ecentricity'] = Dict['ecentricity']+1
        Dict['loose type A'] = Dict['loose type A'] +1
        Dict['gear tooth'] = Dict['gear tooth']+1
        #print("unbalance OR ecentricity OR loose type A OR gear tooth")
   
    if (q_a == 1 and r_a == 0 ):
        #fault.extend([3,4,5])
        Dict['gear tooth'] = Dict['gear tooth']+1
        Dict['overhang unbalance'] = Dict['overhang unbalance']+1
        Dict['belt drive misalignment'] = Dict['belt drive misalignment']+1
        #print("gear tooth OR overhang unbalance OR belt drive misalignment")
    
    if(q_r > 1 and r_r == 0 and (q_r).is_integer()==True):
        #fault.append(6)
        Dict['misalignment offset'] = Dict['misalignment offset']+1
        #print("misalignment offset")
    
    if(q_a > 1 and r_a == 0 and (q_a).is_integer()==True):
        #fault.extend([7,8,9,10])
        Dict['cocked bearing inner'] = Dict['cocked bearing inner']+1
        Dict['cocked bearing outer'] = Dict['cocked bearing outer']+1
        Dict['bent rotors'] = Dict['bent rotors']+1
        Dict['misalignment angular'] = Dict['misalignment angular']+1
        #print("cocked bearing inner/outer OR bent rotors OR misalignment angular")
    
    if((q_a > 1 and r_a == 0 and (q_a).is_integer()==True) or (q_r > 1 and r_r == 0 and (q_r).is_integer()==True) ):
        #fault.extend([11,12,13])
        Dict['Gear mesh'] = Dict['Gear mesh']+1
        Dict['rotor bar'] = Dict['rotor bar']+1
        Dict['Blade Pass'] = Dict['Blade Pass']+1
        #print("Gear mesh OR rotor bar OR Blade Pass")
    
    if(q_r < 1 and (q_r).is_integer()==False):
        #fault.extend([14,15,16,17])
        Dict['Loose type B'] = Dict['Loose type B']+1
        Dict['Rub Event'] = +1
        Dict['Oil Whirl/Whip'] = +1
        Dict['Belt Drive Frequency'] = +1
        #print("Loose type B OR Rub Event OR Oil Whirl/Whip OR Belt Drive Frequency")
    
    if(q_a < 1 and (q_a).is_integer()==False):
        #fault.extend([18])
        Dict['Rolling Element Bearing Cage Frequency'] = +1
        #print("Rolling Element Bearing Cage Frequency")
    
    if((q_a < 1 and (q_a).is_integer()==False) or (q_r < 1 and (q_r).is_integer()==False) ):
        #fault.extend([19,20,21,22,23])
        Dict['Loose Type C'] = +1
        Dict['Gear Tooth hunting'] = +1
        Dict['assembly'] = +1
        Dict['Flow turbulance/cavitation'] = +1
        Dict['electrical pole'] = +1
        #print("Loose Type C OR Gear Tooth hunting or assembly OR Flow turbulance/cavitation OR electrical pole")
    
    if(q_r > 1 and (q_r).is_integer()==False):
        #fault.extend([16,17,24,25])
        Dict['Oil Whirl/Whip'] = +1
        Dict['Belt Drive Frequency'] = +1
        Dict['Rolling Element Bearing outer race'] = +1
        Dict['Rolling Element Bearing inner race'] = +1
        #print("Oil Whirl/Whip OR Belt Drive Frequency OR Rolling Element Bearing outer race/ inner race")
    
#current rpm
radial = 1784
axial = 1784

#machine default rpm
rpm = 1784

i = 0
q_r = radial/rpm
r_r = radial%rpm

q_a = axial/rpm
r_a = axial%rpm

while i < 10:
    fault_detect(q_r , r_r , q_a ,r_a)
    i += 1
    if Dict['count'] == 11:
        Dict['count'] = 1

for index, (key, value) in enumerate(Dict.items()):
    print('Index:: ', index, ' :: ', key, '-', value)

fin_max = max(Dict, key=Dict.get)
print("Fault Detected:",fin_max)
