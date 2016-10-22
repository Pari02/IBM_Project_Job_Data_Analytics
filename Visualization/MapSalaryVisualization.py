
# coding: utf-8

# In[1]:

# geo_map_world() functions in this notebook is referenced from the notebook created by Dan Liu
# the link to the notebook is in the next line
# https://apsportal.ibm.com/analytics/notebooks/82160445-33a4-47ee-9d15-deaa058ae460/view?projectid=aaf120f6-78d3-4028-8235-2d56e431c278


# In[2]:

# add sqlContext
sqlContext = SQLContext(sc)


# In[3]:

# import required packages
from bokeh.io import output_notebook, push_notebook, show
from bokeh.models.widgets import Panel, Tabs
from bokeh.models import (
    ColumnDataSource, HoverTool, ColorBar
    , LabelSet, TapTool, LogTicker, BasicTicker
    , LinearColorMapper, Legend,
)
from bokeh.palettes import Blues9 as palette, Spectral
from bokeh.plotting import figure, hplot
from bokeh.sampledata.us_states import data as states_dict
import numpy as np, requests, json, pandas as pd, itertools


# In[4]:

# function to create world map coordinates
def get_geo_world():
    url = 'https://raw.githubusercontent.com/johan/world.geo.json/master/countries.geo.json'
    r = requests.get(url)
    geo_json_data = r.json()
    features = geo_json_data['features']
    depth = lambda L: isinstance(L, list) and max(map(depth, L))+1
    xs = []
    ys = []
    names = []
    for feature in features:
        name = feature['properties']['name']
        coords = feature['geometry']['coordinates']
        nbdims = depth(coords)
        # one border
        if nbdims == 3:
            pts = np.array(coords[0], 'f')
            xs.append(pts[:, 0])
            ys.append(pts[:, 1])
            names.append(name)
        # several borders
        else:
            for shape in coords:
                pts = np.array(shape[0], 'f')
                xs.append(pts[:, 0])
                ys.append(pts[:, 1])
                names.append(name)
    source = ColumnDataSource(data=dict(x = xs, y = ys, name = names,))
    return source


# In[5]:

# function to extract data from required tables
def get_data():
    
    occGrpSal = pd.DataFrame()
    # save the link to connect to dashDB
<<<<<<< HEAD:Visualization/MapSalaryVisualization.py
    url = "jdbc:db2://hostname:port/BLUDB:user=userId;password=password;"
=======
    url = "jdbc:db2://**.**.**.**:*****/BLUDB:user=***;password=****;"
>>>>>>> origin/master:MapSalaryVisualization.py
    
    # extract the required tables from dashDB
    stateareas = sqlContext.read.jdbc(url, 'STATEAREAS')
    occu_grp = sqlContext.read.jdbc(url, 'OCCUPATIONGROUPsBYSTATE')
    
    # convert the tables to pandas dataframe
    occu_grpDF = occu_grp.toPandas()
    stateareasDF = stateareas.toPandas()

    # rename id value to AREAID in stateareaDF
    stateareasDF.columns.values[3] = 'AREAID'

    # merge the occupation group dataframe and statearea dataframe on areaID
    occGrpData = occu_grpDF.merge(stateareasDF, on='AREAID')

    # create subset
    occu_data_col = ['stateName', 'stateAreaName', 'NAME', 'SALARYTICKERREALTIME', 'SALARY25TH', 'SALARY75TH', 'SALARYAVERAGE', 'SALARYREALTIME25TH', 'SALARYREALTIME75TH', 'SALARYREALTIMEAVERAGE']
    occGrpSal = occGrpData[occu_data_col]
    
    # Split stateAreaName column into STATECODE and STATEAREA
    func = lambda x: pd.Series([i for i in reversed(x.split(','))])
    stateDF = occGrpSal['stateAreaName'].apply(func)
    stateDF.rename(columns={0:'STATECODE',1:'STATEAREA'},inplace=True)

    # merge the newly split column dataframe to occGrpSal
    occGrpSal = pd.concat([occGrpSal, stateDF], axis=1)
    
    # drop the rows where STATECODE = 'All States' & STATECODE = 'All Areas'
    occGrpSal = occGrpSal.drop(occGrpSal[occGrpSal.STATECODE == 'All States'].index)
    occGrpSal = occGrpSal.drop(occGrpSal[occGrpSal.STATECODE == ' All Areas'].index)
    
    return occGrpSal


# In[6]:

def get_States_Sal(states_dict):
    # call get_Data() function to get salary data
    salStateData = get_data()
    
    # extract states lat and lon information for generating map
    states = {code: state for code, state in states_dict.items()}
    
    # sort the data by state names
    states_Name = sorted(states.values(), key = lambda x : x['name'])

    state_xs = [state["lons"] for state in states_Name]
    state_ys = [state["lats"] for state in states_Name]
    state_names = [state["name"] for state in states_Name]

    # create column data source
    source = ColumnDataSource(data=dict(x = state_xs, y = state_ys,
        stateN = [name for name in state_names]))
    
    # get average of salary occupation group in each state
    salStateAgg = pd.DataFrame(salStateData.groupby(['STATECODE', 'stateName'], axis=0, as_index=False)['SALARYAVERAGE', 'SALARYREALTIMEAVERAGE'].mean()).reset_index()
    
    # convert float point to interger
    salStateAgg['SALARYAVERAGE'] = salStateAgg['SALARYAVERAGE'].astype(int)
    salStateAgg['SALARYREALTIMEAVERAGE'] = salStateAgg['SALARYREALTIMEAVERAGE'].astype(int)
    
    # Create colorMap dictionary
    #keys = tuple(pd.unique(salStateAgg["SALARYREALTIMEAVERAGE"]))
    #values = tuple(["#000000", "#FFFF00", "#1CE6FF", "#FF34FF", "#FF4A46", "#008941", "#006FA6", "#A30059",
    #    "#FFDBE5", "#7A4900", "#0000A6", "#63FFAC", "#B79762", "#004D43", "#8FB0FF", "#997D87",
    #    "#5A0007", "#809693", "#FEFFE6", "#1B4400", "#4FC601", "#3B5DFF", "#4A3B53", "#FF2F80",
    #    "#61615A", "#BA0900", "#6B7900", "#00C2A0", "#FFAA92", "#FF90C9", "#B903AA", "#D16100",                
    #    "#DDEFFF", "#000035", "#7B4F4B", "#A1C299", "#300018", "#0AA6D8", "#013349", "#00846F",
    #    "#372101", "#FFB500", "#C2FFED", "#A079BF", "#CC0744", "#C0B9B2", "#C2FF99", "#001E09",
    #    "#00489C", "#6F0062", "#0CBD66", "#EEC3FF"])
    #values = palette
    #colorMap = dict(itertools.izip(keys, values))
    
    # add values to the source and colorDict
    source.add(data = salStateAgg["STATECODE"], name = 'statecode')
    source.add(data = salStateAgg["SALARYREALTIMEAVERAGE"], name = 'salRealAvg')
    source.add(data = salStateAgg["SALARYAVERAGE"], name = 'salAvg')
    #source.add(data = [colorMap[x] for x in salStateAgg["SALARYREALTIMEAVERAGE"]], name = 'type_color')
    
    return source


# In[7]:

def get_OccGrp_Data():
    salOccData = get_data()
    
    # get average of salary occupation group in each state
    occGrpDataAgg = pd.DataFrame(salOccData.groupby(['NAME', 'STATECODE', 'stateName'], axis=0, as_index=False)['SALARYAVERAGE', 'SALARYREALTIMEAVERAGE'].mean()).reset_index()
    
    # get top 10 salaries in occupation group and in each state
    occGrpStateTop10 = (occGrpDataAgg.assign(rn=occGrpDataAgg.sort_values(['SALARYREALTIMEAVERAGE', 'SALARYAVERAGE'], ascending=False).groupby(['stateName']).cumcount() + 1).query('rn <= 10').sort_values(['stateName','rn']))
    
    # convert float point to interger
    occGrpStateTop10['SALARYAVERAGE'] = occGrpStateTop10['SALARYAVERAGE'].astype(int)
    occGrpStateTop10['SALARYREALTIMEAVERAGE'] = occGrpStateTop10['SALARYREALTIMEAVERAGE'].astype(int)
    
    # Create colorMap dictionary
    #keys = tuple(pd.unique(occGrpStateTop10.SALARYREALTIMEAVERAGE))
    #values = tuple(["#000000", "#FFFF00", "#1CE6FF", "#FF34FF", "#FF4A46", "#008941", "#006FA6", "#A30059",
    #    "#FFDBE5", "#7A4900", "#0000A6", "#63FFAC", "#B79762", "#004D43", "#8FB0FF", "#997D87",
    #    "#5A0007", "#809693", "#FEFFE6", "#1B4400", "#4FC601", "#3B5DFF", "#4A3B53", "#FF2F80",
    #    "#61615A", "#BA0900", "#6B7900", "#00C2A0", "#FFAA92", "#FF90C9", "#B903AA", "#D16100",                
    #    "#DDEFFF", "#000035", "#7B4F4B", "#A1C299", "#300018"]) 
    #values = BrBG[10]
    #colorMap = dict(itertools.izip(keys, values))
    
    # extract distinct statenames and ranks and convert numeric data to string
    stateName = [str(x) for x in list(pd.unique(occGrpStateTop10.stateName))]
    ranks = [str(x) for x in sorted(list(pd.unique(occGrpStateTop10.rn)))]
    occGrpStateTop10["SALARYREALTIMEAVERAGE"] = occGrpStateTop10["SALARYREALTIMEAVERAGE"].astype(str)
    occGrpStateTop10["SALARYAVERAGE"] = occGrpStateTop10["SALARYAVERAGE"].astype(str)
    
    # create data dictionary for visualization
    source = ColumnDataSource(
    data=dict(
        salRank=[str(x) for x in occGrpStateTop10["rn"]],
        stCode=[str(y) for y in occGrpStateTop10["stateName"]],
        symx=[str(x)+":0.1" for x in occGrpStateTop10["rn"]],
        rsa=[str(x)+":0.8" for x in occGrpStateTop10["rn"]],
        sa=[str(x)+":0.15" for x in occGrpStateTop10["rn"]],
        namey=[str(x)+":0.3" for x in occGrpStateTop10["rn"]],
        name=occGrpStateTop10["NAME"],
        realSalAvg=occGrpStateTop10["SALARYREALTIMEAVERAGE"],
        salAvg=occGrpStateTop10["SALARYAVERAGE"],
        #type_color=[colorMap[x] for x in occGrpStateTop10["SALARYREALTIMEAVERAGE"]],
        )
    )
    return source, ranks, stateName


# In[8]:

# get data source dictionary
sourceState = get_States_Sal(states_dict)
sourceOccGrp, ranks, stateNm = get_OccGrp_Data()

# save Tool operation in a variable
TOOLS = "pan, wheel_zoom, box_zoom, reset, save, tap"


# In[9]:

# pass parameters to generate the map for avaerage salaries
mp = figure(tools=TOOLS, toolbar_location="above",
    x_axis_location=None, y_axis_location=None, 
    plot_width=1200, plot_height=1100, x_range=(-180,-65), y_range=(6,75)
)
mp.grid.grid_line_color = None
mp.title.text_font_size = '20pt'

#print world map as background
mp1 = mp.patches('x', 'y', source=get_geo_world(),
    fill_color='white', fill_alpha=0.7, line_width=0.5)

# reverse the color list so that dark represents higer salary
#pl = tuple(reversed(palette))

# add the salary data to the map
mp2 = mp.patches('x', 'y', source=sourceState, #legend = 'statecode',
                 fill_color={'field': 'salRealAvg', 'transform': LinearColorMapper(palette=palette)},
                 fill_alpha=0.7, line_color='grey', line_width=0.5)

# add lables to map
labels = LabelSet(x='x', y='y', text='statecode', level='glyph', source=sourceState)
mp.add_layout(labels)

# add color bar for salary Realtime average
color_map = LinearColorMapper(palette= palette, 
                              low = min(sourceState.data['salRealAvg']),
                              high = max(sourceState.data['salRealAvg']))
color_bar = ColorBar(color_mapper=color_map,ticker=BasicTicker(), title='Sal Real Avg'
                     , label_standoff=12, border_line_color=None, location=(0,0))
mp.add_layout(color_bar, 'left')

# add hover text
mp.add_tools(HoverTool(renderers=[mp2],
    point_policy = "follow_mouse",
    tooltips = [
    ("State", "@stateN"),
        ("Salary RealTime Average", "$"+"@salRealAvg"),
        ("Salary Average", "$"+"@salAvg"),
        ("(Lon, Lat)", "($x, $y)"),]))

# add legend properties
#mp.legend.location= 'top_left'
#mp.legend.label_text_font_size = '7pt'
#mp.legend.glyph_height = 8
#mp.legend.label_text_baseline = 'ideographic'
#mp.legend.label_text_font_style = 'bold'
#mp.legend.spacing = 0
#mp.legend.margin = 0

tab1 = Panel(child=mp, title="Salary Average Distribution")


# In[10]:

# pass parameters for top 10 salary by Occupation group and State
p = figure(tools=TOOLS, toolbar_location="above", 
           x_range=ranks, y_range=list(reversed(stateNm)),
           plot_width=3000, plot_height=2000)

p.outline_line_color = None


pl = Spectral[10]
p.rect("salRank", "stCode", .97, .97, source=sourceOccGrp,
       fill_color={'field': 'salRank', 'transform': LinearColorMapper(palette=pl)},
       fill_alpha=0.5)
p.xaxis[0].axis_label = "Ranks"
p.yaxis[0].axis_label = "States"

text_props = {
    "source": sourceOccGrp,
    "angle": 0,
    "color": "black",
    "text_align": "left",
    "text_baseline": "middle"
}

p.text(x="symx", y="stCode", text="name",
       text_font_style="bold", text_font_size="9pt", **text_props)

p.grid.grid_line_color = None

p.add_tools(HoverTool(
        point_policy = "follow_mouse",
        tooltips = [
            ("name", "@name"),
            ("Salary Realtime Average", "@realSalAvg"),
            ("Salary Average", "@salAvg"),
            ("State", "@stCode"),]))

# add color bar for salary Realtime average
color_mapOcc = LinearColorMapper(palette= list(reversed(pl)))
color_barOcc = ColorBar(color_mapper=color_mapOcc,ticker=BasicTicker(), title='High-Low: 1 to 10'
                     , label_standoff=12, border_line_color=None, location=(0,0))
p.add_layout(color_barOcc, 'left')

# create a panel for tab1
tab2 = Panel(child=p, title="Top 10 Salary Average and Salary Realtime Avegare by Occupation Group - State")


# In[11]:

# combine the tabs
tabs = Tabs(tabs=[tab1, tab2])


# In[12]:

#Display data
output_notebook()

show(tabs)


# In[13]:

get_ipython().run_cell_magic(u'javascript', u'', u"require.config({\n  paths: {\n      d3: '//cdnjs.cloudflare.com/ajax/libs/d3/3.4.8/d3.min'\n  }\n});")

